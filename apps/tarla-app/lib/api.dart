import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:crypto/crypto.dart';

typedef Row = Map<String, dynamic>;

/// Direct connections use the native trust store. Cleartext is limited to local
/// development endpoints; a private mesh can expose an HTTPS application proxy.
class Endpoint {
  final Uri uri;
  final String token;
  Endpoint(String address, this.token) : uri = parse(address) {
    if (token.trim().isEmpty) throw const FormatException('请输入访问令牌');
  }

  static Uri parse(String value) {
    final u = Uri.tryParse(value.trim());
    if (u == null ||
        !u.hasAuthority ||
        u.host.isEmpty ||
        u.userInfo.isNotEmpty ||
        u.hasQuery ||
        u.hasFragment ||
        (u.path.isNotEmpty && u.path != '/')) {
      throw const FormatException('请输入服务地址，例如 https://tarla.example.com');
    }
    final local = [
      'localhost',
      '127.0.0.1',
      '::1',
      '10.0.2.2',
    ].contains(u.host);
    if (u.scheme != 'https' && !(local && u.scheme == 'http')) {
      throw const FormatException('远程连接需要 HTTPS，本机调试可用 HTTP');
    }
    return u.replace(path: '');
  }

  Uri route(List<String> parts) => uri.replace(pathSegments: ['v1', ...parts]);
}

class Profile {
  final String name;
  final Endpoint supervisor;
  final Endpoint? executor;
  Profile(this.name, this.supervisor, [this.executor]);
  static bool validId(String id) =>
      RegExp(r'^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$').hasMatch(id);
}

class ApiError implements Exception {
  final int status;
  final String message;
  ApiError(this.status, this.message);
  @override
  String toString() => 'HTTP $status · $message';
}

bool unavailable(Object error) =>
    error is SocketException ||
    error is TimeoutException ||
    error is HttpException ||
    (error is ApiError && [502, 503, 504].contains(error.status));

String identifier() {
  final random = Random.secure();
  return List.generate(
    16,
    (_) => random.nextInt(256).toRadixString(16).padLeft(2, '0'),
  ).join();
}

class TarlaApi {
  final Endpoint endpoint;
  final String? workspaceId;
  bool _verified = false;
  final HttpClient _client = HttpClient()
    ..connectionTimeout = const Duration(seconds: 7);
  TarlaApi(this.endpoint, {this.workspaceId});

  Future<dynamic> request(
    String method,
    List<String> path, [
    Row? payload,
    Map<String, String>? query,
  ]) async {
    if (workspaceId != null && !_verified) {
      final health = await _exchange(
        'GET',
        endpoint.uri.replace(path: '/health'),
        null,
        authenticate: false,
      );
      if (health is! Map || health['workspace_id'] != workspaceId) {
        throw ApiError(409, '服务端工作空间与选择的 ID 不一致');
      }
      _verified = true;
    }
    return _exchange(
      method,
      endpoint.route(path).replace(queryParameters: query),
      payload,
    );
  }

  Future<dynamic> _exchange(
    String method,
    Uri uri,
    Row? payload, {
    bool authenticate = true,
  }) async {
    final req = await _client.openUrl(method, uri);
    req.followRedirects = false;
    if (authenticate) {
      req.headers.set(
        HttpHeaders.authorizationHeader,
        'Bearer ${endpoint.token}',
      );
      if (workspaceId != null)
        req.headers.set('X-Tarla-Workspace', workspaceId!);
    }
    req.headers.set(HttpHeaders.acceptHeader, 'application/json');
    if (payload != null) {
      req.headers.contentType = ContentType.json;
      req.write(jsonEncode(payload));
    }
    final timeout = method == 'GET' || uri.pathSegments.last == 'attach'
        ? const Duration(seconds: 10)
        : const Duration(seconds: 120);
    final res = await req.close().timeout(
      timeout,
      onTimeout: () {
        req.abort();
        throw TimeoutException('服务响应超时');
      },
    );
    final bytes = await res
        .fold<List<int>>([], (b, v) {
          if (b.length + v.length > 8 * 1024 * 1024) {
            throw const FormatException('响应过大');
          }
          return b..addAll(v);
        })
        .timeout(
          timeout,
          onTimeout: () {
            req.abort();
            throw TimeoutException('服务响应超时');
          },
        );
    final raw = utf8.decode(bytes, allowMalformed: true);
    if (res.statusCode < 200 || res.statusCode >= 300) {
      String message = res.reasonPhrase;
      try {
        final body = jsonDecode(raw);
        if (body is Map)
          message = '${body['error'] ?? body['message'] ?? message}';
      } on FormatException {
        // A proxy may have returned HTML instead of a structured daemon error.
      }
      throw ApiError(res.statusCode, message);
    }
    return raw.isEmpty ? null : jsonDecode(raw);
  }

  Future<List<Row>> list(
    List<String> path, {
    Map<String, String>? query,
  }) async => (await request('GET', path, null, query) as List).cast<Row>();
  Future<Row> get(List<String> path, {Map<String, String>? query}) async =>
      await request('GET', path, null, query) as Row;
  Future<dynamic> post(List<String> path, Row data) =>
      request('POST', path, data);

  Future<Row> _grant(String id, String mode, String route) async =>
      await post(['runs', id, 'attach'], {'mode': mode, 'route': route}) as Row;

  Future<WebSocket> terminal(
    String id,
    String mode, {
    TarlaApi? offlineExecutor,
  }) async {
    try {
      final lease = await _grant(id, mode, 'auto');
      final target = lease['node_url'] as String?;
      if (target == null || target.isEmpty) {
        return await _upgrade(id, mode, lease, endpoint.uri);
      }
      final direct = Endpoint.parse(target);
      if (endpoint.uri.scheme == 'https' && direct.scheme != 'https') {
        throw const FormatException('执行节点地址需要受信任的 HTTPS 连接');
      }
      try {
        return await _upgrade(id, mode, lease, direct);
      } catch (error) {
        if (!unavailable(error)) rethrow;
        final fresh = await _grant(id, mode, 'supervisor');
        if (fresh['node_url'] != null && fresh['node_url'] != '') {
          throw const FormatException('主管未返回本站中转票据');
        }
        return await _upgrade(id, mode, fresh, endpoint.uri);
      }
    } catch (error) {
      if (offlineExecutor == null || !unavailable(error)) rethrow;
      return offlineExecutor.terminal(id, mode);
    }
  }

  Future<WebSocket> _upgrade(
    String id,
    String mode,
    Row lease,
    Uri base,
  ) async {
    if ((lease['run_id'] != null && lease['run_id'] != id) ||
        (lease['mode'] != null && lease['mode'] != mode)) {
      throw const FormatException('接管票据与本次任务或权限不一致');
    }
    final url = base.replace(
      scheme: base.scheme == 'https' ? 'wss' : 'ws',
      pathSegments: ['v1', 'terminal', id],
    );
    final ticket = lease['ticket'];
    if (ticket is! String || ticket.isEmpty)
      throw const FormatException('接管票据为空');
    // Open the upgrade ourselves, so the ticket cannot follow HTTP redirects.
    final key = base64.encode(
      List.generate(16, (_) => Random.secure().nextInt(256)),
    );
    final req = await _client.getUrl(url.replace(scheme: base.scheme));
    req.followRedirects = false;
    req.headers
      ..set('Connection', 'Upgrade')
      ..set('Upgrade', 'websocket')
      ..set('Sec-WebSocket-Key', key)
      ..set('Sec-WebSocket-Version', '13')
      ..set('Authorization', 'Bearer $ticket');
    final res = await req.close().timeout(
      const Duration(seconds: 10),
      onTimeout: () {
        req.abort();
        throw TimeoutException('终端握手超时');
      },
    );
    if (res.statusCode != HttpStatus.switchingProtocols ||
        res.headers.value('upgrade')?.toLowerCase() != 'websocket' ||
        res.headers.value('sec-websocket-accept') !=
            base64.encode(
              sha1
                  .convert(
                    ascii.encode('${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11'),
                  )
                  .bytes,
            )) {
      req.abort();
      throw ApiError(res.statusCode, '终端接管失败');
    }
    return WebSocket.fromUpgradedSocket(
      await res.detachSocket(),
      serverSide: false,
    );
  }

  void close() => _client.close(force: true);
}
