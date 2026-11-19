import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:crypto/crypto.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'api.dart';

const _channel = MethodChannel('dev.tarla/local-core');

class LocalCoreButton extends StatefulWidget {
  final ValueChanged<Profile> onConnect;
  const LocalCoreButton({super.key, required this.onConnect});
  @override
  State<LocalCoreButton> createState() => _LocalCoreButtonState();
}

class _LocalCoreButtonState extends State<LocalCoreButton> {
  late final Future<bool> _available;
  @override
  void initState() {
    super.initState();
    _available = _check();
  }

  Future<bool> _check() async {
    if (!Platform.isMacOS) return false;
    try {
      return await _channel.invokeMethod<String>('status') != 'unavailable';
    } on MissingPluginException {
      return false;
    } on PlatformException {
      return false;
    }
  }

  @override
  Widget build(BuildContext context) => FutureBuilder<bool>(
    future: _available,
    builder: (_, value) => value.data == true
        ? IconButton(
            tooltip: '管理本机服务',
            icon: const Icon(Icons.computer),
            onPressed: () async {
              final profile = await showDialog<Profile>(
                context: context,
                builder: (_) => const _LocalServiceDialog(),
              );
              if (mounted && profile != null) widget.onConnect(profile);
            },
          )
        : const SizedBox.shrink(),
  );
}

/// Challenge the listener before giving it any bearer credential. A stale port
/// after a crash may already belong to a different local application.
Future<Profile> verifyLocalService(int port, String token) async {
  if (port <= 0 || port > 65535 || token.length < 32) {
    throw const FormatException('本机服务连接文件无效');
  }
  final random = Random.secure();
  final nonce = List.generate(
    32,
    (_) => random.nextInt(256).toRadixString(16).padLeft(2, '0'),
  ).join();
  final origin = Uri(scheme: 'http', host: '127.0.0.1', port: port);
  final client = HttpClient()..findProxy = (_) => 'DIRECT';
  Future<Profile> probe() async {
    final req = await client.getUrl(origin.resolve('/local/v1/proof/$nonce'));
    req.followRedirects = false;
    final response = await req.close();
    if (response.statusCode != 200) throw const FormatException('本机服务身份验证失败');
    final bytes = <int>[];
    await for (final part in response) {
      if (bytes.length + part.length > 512)
        throw const FormatException('本机服务响应无效');
      bytes.addAll(part);
    }
    final body = jsonDecode(utf8.decode(bytes));
    final expected = Hmac(
      sha256,
      utf8.encode(token),
    ).convert(utf8.encode('tarla-desktop-v1:$nonce')).toString();
    if (body is! Map || body['proof'] != expected) {
      throw const FormatException('本机服务身份验证失败');
    }
    return Profile('desktop-local', Endpoint(origin.toString(), token));
  }

  try {
    return await probe().timeout(const Duration(seconds: 4));
  } finally {
    client.close(force: true);
  }
}

class _LocalServiceDialog extends StatefulWidget {
  const _LocalServiceDialog();
  @override
  State<_LocalServiceDialog> createState() => _LocalServiceDialogState();
}

class _LocalServiceDialogState extends State<_LocalServiceDialog> {
  String? status, error;
  bool busy = false;
  @override
  void initState() {
    super.initState();
    _perform('status');
  }

  Future<void> _perform(String operation) async {
    if (busy) return;
    setState(() {
      busy = true;
      error = null;
    });
    try {
      if (operation == 'connect') {
        final config = await _channel.invokeMapMethod<String, dynamic>(
          'connection',
        );
        final port = config?['port'], token = config?['token'];
        if (port is! int || token is! String)
          throw const FormatException('本机服务尚未就绪');
        final profile = await verifyLocalService(port, token);
        if (mounted) Navigator.pop(context, profile);
      } else {
        if (operation == 'openSettings')
          await _channel.invokeMethod<void>(operation);
        final updated = await _channel.invokeMethod<String>(
          operation == 'openSettings' ? 'status' : operation,
        );
        if (mounted) setState(() => status = updated);
      }
    } catch (e) {
      if (mounted)
        setState(
          () => error = e is PlatformException
              ? e.message ?? e.code
              : e.toString(),
        );
    } finally {
      if (mounted) setState(() => busy = false);
    }
  }

  @override
  Widget build(BuildContext context) => AlertDialog(
    title: const Text('本机服务'),
    content: Column(
      mainAxisSize: MainAxisSize.min,
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(switch (status) {
          'enabled' => 'macOS 已启用本机后台服务。',
          'approval' => '请在系统设置的「登录项与扩展」中允许 Tarla 在后台运行，然后刷新状态。',
          'disabled' => '启用后，macOS 会启动 desktop-local 工作空间，并在你登录时自动运行。',
          'missing' => 'macOS 找不到后台服务。请从「应用程序」目录打开完整的 Tarla Local 应用。',
          null => '正在读取服务状态…',
          _ => 'macOS 服务状态未知。',
        }),
        if (error != null)
          Text(
            error!,
            style: TextStyle(color: Theme.of(context).colorScheme.error),
          ),
      ],
    ),
    actions: [
      TextButton(
        onPressed: busy ? null : () => Navigator.pop(context),
        child: const Text('关闭'),
      ),
      if (status == 'approval')
        TextButton(
          onPressed: busy ? null : () => _perform('openSettings'),
          child: const Text('打开系统设置'),
        ),
      if (status == 'enabled' || status == 'approval')
        TextButton(
          onPressed: busy ? null : () => _perform('unregister'),
          child: const Text('停用服务'),
        ),
      if (status == 'disabled')
        FilledButton(
          onPressed: busy ? null : () => _perform('register'),
          child: const Text('启用服务'),
        ),
      if (status == 'enabled')
        FilledButton(
          onPressed: busy ? null : () => _perform('connect'),
          child: const Text('连接本机'),
        ),
      if (status == 'approval' || error != null)
        TextButton(
          onPressed: busy ? null : () => _perform('status'),
          child: const Text('刷新状态'),
        ),
    ],
  );
}
