import 'dart:async';
import 'dart:io';

import 'package:tarla_app/api.dart';
import 'package:test/test.dart';

void main() {
  final binaries = Platform.environment['TARLA_TEST_BIN'];
  test(
    'client reads and mutates a private workspace on an actual daemon',
    () async {
      final root = await Directory.systemTemp.createTemp('tarla-app-contract-');
      final ext = Platform.isWindows ? '.exe' : '';
      final cli = '$binaries/tarla$ext';
      final daemon = '$binaries/tarlad$ext';
      final args = ['--home', root.path, '--workspace', 'work'];
      Process? child;
      TarlaApi? client;
      try {
        final init = await Process.run(cli, [
          ...args,
          'init',
          '--node',
          'chief',
          '--role',
          'supervisor_and_executor',
          '--supervisor',
          'chief',
        ]);
        expect(init.exitCode, 0, reason: init.stderr.toString());
        final token = (await File(
          '${root.path}/workspaces/work/runtime/api.token',
        ).readAsString()).trim();
        final port = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
        final address = '127.0.0.1:${port.port}';
        await port.close();
        child = await Process.start(daemon, [...args, '--listen', address]);
        final service = TarlaApi(
          Endpoint('http://$address', token),
          workspaceId: 'work',
        );
        client = service;
        final end = DateTime.now().add(const Duration(seconds: 15));
        while (true) {
          try {
            await service.list(['runs']);
            break;
          } on SocketException {
            if (DateTime.now().isAfter(end)) rethrow;
          }
          await Future<void>.delayed(const Duration(milliseconds: 80));
        }
        final wrong = TarlaApi(
          Endpoint('http://$address', token),
          workspaceId: 'life',
        );
        try {
          await expectLater(
            wrong.post(
              ['memories'],
              {'memory_id': 'wrong', 'content': 'wrong', 'base_version': 0},
            ),
            throwsA(isA<ApiError>().having((e) => e.status, 'status', 409)),
          );
        } finally {
          wrong.close();
        }
        for (final group in [
          'conversations',
          'runs',
          'actions',
          'memories',
          'facts',
        ]) {
          expect(await service.list([group]), isEmpty);
        }
        await service.request(
          'PUT',
          ['conversations', 'thread', 'binding'],
          {'mode': 'orchestrator'},
        );
        expect(
          (await service.list(['conversations'])).single['conversation_id'],
          'thread',
        );
        await service.post(
          ['memories'],
          {
            'memory_id': 'project-convention',
            'scope': 'global',
            'content': 'Use UTC in exports',
            'base_version': 0,
          },
        );
        final memory = (await service.list(['memories'])).single;
        expect(memory['content'], 'Use UTC in exports');
        expect(memory['version'], 1);
        await service.post(
          ['memories'],
          {
            'memory_id': 'project-convention',
            'scope': 'global',
            'content': 'Use local time',
            'base_version': 0,
          },
        );
        final conflict = (await service.list(['actions'])).single;
        expect(conflict['kind'], 'memory_conflict');
        await service.post(
          ['actions', conflict['id']],
          {'resolution': 'rejected', 'expected_version': 1},
        );
        expect(await service.list(['actions']), isEmpty);
        expect(
          (await service.list(['memories'])).single['content'],
          'Use UTC in exports',
        );
      } finally {
        client?.close();
        if (child != null) {
          child.kill();
          await child.exitCode.timeout(
            const Duration(seconds: 5),
            onTimeout: () {
              child!.kill(ProcessSignal.sigkill);
              return -1;
            },
          );
        }
        await root.delete(recursive: true);
      }
    },
    skip: binaries == null
        ? 'Set TARLA_TEST_BIN to a directory containing built tarla and tarlad binaries'
        : false,
  );
}
