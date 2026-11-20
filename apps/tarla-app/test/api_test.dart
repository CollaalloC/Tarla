import 'dart:convert';
import 'dart:io';
import 'package:test/test.dart';
import 'package:tarla_app/api.dart';

void main() {
  test(
    'a workspace mismatch blocks the credential and all business requests',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final endpoint = Endpoint(
        'http://127.0.0.1:${server.port}',
        'workspace-secret',
      );
      final wrong = TarlaApi(endpoint, workspaceId: 'life');
      final correct = TarlaApi(endpoint, workspaceId: 'work');
      final paths = <String>[];
      server.listen((req) async {
        paths.add(req.uri.path);
        if (req.uri.path == '/health') {
          expect(req.headers.value('Authorization'), isNull);
          req.response.write(jsonEncode({'workspace_id': 'work'}));
        } else {
          expect(req.headers.value('Authorization'), 'Bearer workspace-secret');
          expect(req.headers.value('X-Tarla-Workspace'), 'work');
          req.response.write('[]');
        }
        await req.response.close();
      });
      try {
        await expectLater(
          wrong.post(['memories'], {'content': 'isolated'}),
          throwsA(isA<ApiError>().having((e) => e.status, 'status', 409)),
        );
        expect(paths, ['/health']);
        expect(await correct.list(['memories']), isEmpty);
        expect(await correct.list(['runs']), isEmpty);
        expect(paths, ['/health', '/health', '/v1/memories', '/v1/runs']);
      } finally {
        wrong.close();
        correct.close();
        await server.close(force: true);
      }
    },
  );

  test(
    'endpoints reject embedded credentials, unsafe remote transport and redirects encoded in URLs',
    () {
      for (final url in [
        'http://example.org',
        'https://u:p@example.org',
        'https://example.org/v1',
        'https://example.org/?ticket=123',
        'file:///tmp/fake',
      ]) {
        expect(() => Endpoint(url, 's'), throwsFormatException);
      }
      expect(
        Endpoint(
          'http://127.0.0.1:8000',
          's',
        ).route(['runs', 'a/b']).toString(),
        'http://127.0.0.1:8000/v1/runs/a%2Fb',
      );
    },
  );

  test('HTTP workspace credentials never follow redirects', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final client = TarlaApi(
      Endpoint('http://127.0.0.1:${server.port}', 'workspace-only'),
    );
    final seen = <String>[];
    server.listen((req) async {
      seen.add(req.uri.path);
      expect(req.headers.value('Authorization'), 'Bearer workspace-only');
      req.response.statusCode = 307;
      req.response.headers.set('Location', '/unexpected');
      await req.response.close();
    });
    try {
      await expectLater(client.list(['runs']), throwsA(isA<ApiError>()));
      expect(seen, ['/v1/runs']);
    } finally {
      client.close();
      await server.close(force: true);
    }
  });

  test(
    'terminal obtains one scoped lease and sends only the lease to the executor',
    () async {
      final supervisor = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final executor = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final client = TarlaApi(
        Endpoint('http://127.0.0.1:${supervisor.port}', 'workspace'),
      );
      supervisor.listen((req) async {
        expect(req.uri.path, '/v1/runs/run-a/attach');
        expect(req.headers.value('Authorization'), 'Bearer workspace');
        expect(jsonDecode(await utf8.decoder.bind(req).join()), {
          'mode': 'observe',
          'route': 'auto',
        });
        req.response.write(
          jsonEncode({
            'ticket': 'lease',
            'node_url': 'http://127.0.0.1:${executor.port}',
          }),
        );
        await req.response.close();
      });
      executor.listen((req) async {
        expect(req.uri.path, '/v1/terminal/run-a');
        expect(req.headers.value('Authorization'), 'Bearer lease');
        final ws = await WebSocketTransformer.upgrade(req);
        ws.add([0, 27, 65]);
        await ws.close();
      });
      try {
        final ws = await client.terminal('run-a', 'observe');
        expect(await ws.first, [0, 27, 65]);
        await ws.close();
      } finally {
        client.close();
        await executor.close(force: true);
        await supervisor.close(force: true);
      }
    },
  );

  test('direct failure obtains a fresh supervisor-scoped lease', () async {
    final supervisor = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final direct = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final client = TarlaApi(
      Endpoint('http://127.0.0.1:${supervisor.port}', 'work-token'),
    );
    final routes = <String>[];
    direct.listen((r) async {
      expect(r.headers.value('Authorization'), 'Bearer direct-only');
      r.response.statusCode = 503;
      await r.response.close();
    });
    supervisor.listen((r) async {
      if (r.uri.path.endsWith('/attach')) {
        expect(r.headers.value('Authorization'), 'Bearer work-token');
        final route =
            jsonDecode(await utf8.decoder.bind(r).join())['route'] as String;
        routes.add(route);
        r.response.write(
          jsonEncode({
            'run_id': 'run',
            'mode': 'interactive',
            'ticket': route == 'auto' ? 'direct-only' : 'fresh-relay',
            if (route == 'auto') 'node_url': 'http://127.0.0.1:${direct.port}',
          }),
        );
        await r.response.close();
      } else {
        expect(r.headers.value('Authorization'), 'Bearer fresh-relay');
        final ws = await WebSocketTransformer.upgrade(r);
        ws.add([42]);
        await ws.close();
      }
    });
    try {
      final ws = await client.terminal('run', 'interactive');
      expect(await ws.first, [42]);
      expect(routes, ['auto', 'supervisor']);
      await ws.close();
    } finally {
      client.close();
      await supervisor.close(force: true);
      await direct.close(force: true);
    }
  });

  test(
    'redirected terminal cannot receive either credential and is not retried',
    () async {
      final chief = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final trap = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var attach = 0, received = 0;
      final client = TarlaApi(
        Endpoint('http://127.0.0.1:${chief.port}', 'space'),
      );
      trap.listen((r) async {
        received++;
        await r.response.close();
      });
      chief.listen((r) async {
        if (r.uri.path.endsWith('/attach')) {
          attach++;
          r.response.write(jsonEncode({'ticket': 'scoped'}));
        } else {
          expect(r.headers.value('Authorization'), 'Bearer scoped');
          r.response.statusCode = 307;
          r.response.headers.set(
            'Location',
            'http://127.0.0.1:${trap.port}/stolen',
          );
        }
        await r.response.close();
      });
      try {
        await expectLater(
          client.terminal('run', 'observe'),
          throwsA(isA<ApiError>().having((e) => e.status, 'status', 307)),
        );
        expect(attach, 1);
        expect(received, 0);
      } finally {
        client.close();
        await chief.close(force: true);
        await trap.close(force: true);
      }
    },
  );

  test(
    'offline supervisor falls back to the prepaired executor credential and workspace',
    () async {
      final down = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final worker = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final chief = TarlaApi(
        Endpoint('http://127.0.0.1:${down.port}', 'chief-only'),
        workspaceId: 'w',
      );
      final paired = TarlaApi(
        Endpoint('http://127.0.0.1:${worker.port}', 'worker-only'),
        workspaceId: 'w',
      );
      await down.close(force: true);
      worker.listen((r) async {
        if (r.uri.path == '/health') {
          expect(r.headers.value('Authorization'), isNull);
          r.response.write('{"workspace_id":"w"}');
        } else if (r.uri.path.endsWith('/attach')) {
          expect(r.headers.value('Authorization'), 'Bearer worker-only');
          expect(r.headers.value('X-Tarla-Workspace'), 'w');
          expect(
            jsonDecode(await utf8.decoder.bind(r).join())['route'],
            'auto',
          );
          r.response.write('{"ticket":"executor-lease"}');
        } else {
          expect(r.headers.value('Authorization'), 'Bearer executor-lease');
          final ws = await WebSocketTransformer.upgrade(r);
          ws.add([7]);
          await ws.close();
          return;
        }
        await r.response.close();
      });
      try {
        final ws = await chief.terminal(
          'run',
          'observe',
          offlineExecutor: paired,
        );
        expect(await ws.first, [7]);
        await ws.close();
      } finally {
        chief.close();
        paired.close();
        await worker.close(force: true);
      }
    },
  );
}
