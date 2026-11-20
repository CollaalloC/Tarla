import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/api.dart' as api;
import 'package:tarla_app/main.dart';
import 'package:tarla_app/profiles.dart';
import 'package:tarla_app/screens.dart';

api.Profile profile(String id, [String? fallback]) => api.Profile(
      id,
      api.Endpoint('https://$id.example.test', '$id-token'),
      fallback == null
          ? null
          : api.Endpoint('https://worker.example.test', fallback),
    );

class MemoryStore implements ProfileStore {
  String? raw;
  MemoryStore(ProfileBook initial) : raw = initial.encode();
  @override
  Future<ProfileBook> load() async =>
      raw == null ? const ProfileBook() : ProfileBook.decode(raw!);
  @override
  Future<void> save(ProfileBook state) async => raw = state.encode();
}

class ReadOnlyRun extends api.TarlaApi {
  ReadOnlyRun() : super(api.Endpoint('https://example.test', 'test'));
  @override
  Future<api.Row> get(List<String> path, {Map<String, String>? query}) async =>
      {
        'run_id': 'r',
        'hc_terminal_id': 'term',
        'terminal_modes': ['observe'],
        'terminal_input_hint': '请在会话中继续发送下一条指令。',
      };
}

void main() {
  test(
    'profile store delegates to a single protected platform record',
    () async {
      TestWidgetsFlutterBinding.ensureInitialized();
      final messenger =
          TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
      String? sealed;
      messenger.setMockMethodCallHandler(DeviceProfileStore.channel, (
        call,
      ) async {
        switch (call.method) {
          case 'read':
            return sealed;
          case 'write':
            sealed = call.arguments as String;
            return null;
          case 'clear':
            sealed = null;
            return null;
        }
        throw PlatformException(code: 'unknown');
      });
      addTearDown(
        () => messenger.setMockMethodCallHandler(
          DeviceProfileStore.channel,
          null,
        ),
      );
      final store = DeviceProfileStore();
      final first = const ProfileBook()
          .select(profile('work', 'independent'))
          .select(profile('life'));
      await store.save(first);
      final fresh = await DeviceProfileStore().load();
      expect(fresh.activeId, 'life');
      expect(fresh.entries.first.executor!.token, 'independent');
      await store.save(fresh.select(profile('work', 'rotated')).remove('life'));
      expect((await store.load()).entries.single.executor!.token, 'rotated');
      await store.save((await store.load()).remove('work'));
      expect(sealed, isNull);
    },
  );

  testWidgets(
    'cold start, update, workspace switch and removal persist independently',
    (tester) async {
      final store = MemoryStore(
        const ProfileBook()
            .select(profile('work', 'worker-secret'))
            .select(profile('life')),
      );
      Future<void> menu(String item) async {
        await tester.tap(find.byTooltip('切换工作空间'));
        await tester.pumpAndSettle();
        final choice = find.ancestor(
          of: find.text(item),
          matching:
              find.byWidgetPredicate((widget) => widget is PopupMenuItem<int>),
        );
        await tester.tap(choice);
        await tester.pumpAndSettle();
      }

      Future<void> restart() async {
        await tester.pumpWidget(const SizedBox.shrink());
        await tester.pumpWidget(TarlaApp(profileStore: store));
        await tester.pumpAndSettle();
      }

      await restart();
      expect(find.text('life'), findsOneWidget);
      await menu('work');
      await menu('编辑当前连接…');
      final fields = find.byType(TextField);
      expect(tester.widget<TextField>(fields.at(0)).readOnly, isTrue);
      await tester.enterText(fields.at(2), 'new-work-token');
      await tester.tap(find.text('保存'));
      await tester.pumpAndSettle();
      await restart();
      expect(find.text('work'), findsOneWidget);
      final book = await store.load();
      expect(book.selected!.supervisor.token, 'new-work-token');
      expect(book.selected!.executor!.token, 'worker-secret');
      expect(
        book.entries.firstWhere((p) => p.name == 'life').supervisor.token,
        'life-token',
      );
      await menu('移除当前配对…');
      await tester.tap(find.text('移除'));
      await tester.pumpAndSettle();
      await restart();
      expect(find.text('life'), findsOneWidget);
      expect((await store.load()).entries.map((p) => p.name), ['life']);
      await tester.pumpWidget(const SizedBox.shrink());
    },
  );

  testWidgets(
    'observe-only terminal shows input hint and disables keyboard takeover',
    (tester) async {
      final client = ReadOnlyRun();
      addTearDown(client.close);
      await tester.pumpWidget(
        MaterialApp(
          home: RunPage(api: client, id: 'r'),
        ),
      );
      await tester.pumpAndSettle();
      final interact = tester.widget<OutlinedButton>(
        find.widgetWithText(OutlinedButton, '接管终端'),
      );
      expect(interact.onPressed, isNull);
      expect(
        tester
            .widget<OutlinedButton>(find.widgetWithText(OutlinedButton, '旁观终端'))
            .onPressed,
        isNotNull,
      );
      expect(find.text('请在会话中继续发送下一条指令。'), findsOneWidget);
      await tester.pumpWidget(const SizedBox.shrink());
    },
  );
}
