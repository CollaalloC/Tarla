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
}
