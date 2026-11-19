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
  final HttpClient _client = HttpClient()
    ..connectionTimeout = const Duration(seconds: 7);
  TarlaApi(this.endpoint);

  Future<dynamic> request(
    String method,
    List<String> path, [
    Row? payload,
    Map<String, String>? query,
  ]) async {
    final req = await _client.openUrl(
      method,
      endpoint.route(path).replace(queryParameters: query),
    );
    req.followRedirects = false;
    req.headers.set(
      HttpHeaders.authorizationHeader,
      'Bearer ${endpoint.token}',
    );
    req.headers.set(HttpHeaders.acceptHeader, 'application/json');
    if (payload != null) {
      req.headers.contentType = ContentType.json;
      req.write(jsonEncode(payload));
    }
    final res = await req.close().timeout(
      const Duration(seconds: 120),
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
        .timeout(const Duration(seconds: 120));
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

  Future<WebSocket> terminal(String id, String mode) async {
    final lease = await post(['runs', id, 'attach'], {'mode': mode}) as Row;
    final target = lease['node_url'] as String?;
    final base = target == null || target.isEmpty
        ? endpoint.uri
        : Endpoint.parse(target);
    final url = base.replace(
      scheme: base.scheme == 'https' ? 'wss' : 'ws',
      pathSegments: ['v1', 'terminal', id],
    );
    final ticket = lease['ticket'] as String;
    if (ticket.isEmpty) throw const FormatException('接管票据为空');
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
    final res = await req.close().timeout(const Duration(seconds: 10));
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
      await res.drain<void>();
      throw ApiError(res.statusCode, '终端接管失败');
    }
    return WebSocket.fromUpgradedSocket(
      await res.detachSocket(),
      serverSide: false,
    );
  }

  void close() => _client.close(force: true);
}
