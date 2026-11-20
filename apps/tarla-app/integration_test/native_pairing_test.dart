import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:xterm/xterm.dart';

import 'native_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'two isolated workspaces, archived history and a scoped remote PTY',
    (tester) async {
      await openApp(tester);
      await tap(tester, find.text('连接工作空间'));
      await addProfile(tester, 'work', workUrl, workToken);
      await visible(tester, find.textContaining('mobile-archive ·'));
      await tap(tester, find.textContaining('mobile-archive ·'));
      await tap(tester, find.text('加载更早的消息'));
      final scroll = find
          .descendant(
            of: find.byType(ListView),
            matching: find.byType(Scrollable),
          )
          .first;
      await tester.scrollUntilVisible(
        find.text(archiveFirst),
        -400,
        scrollable: scroll,
        maxScrolls: 80,
      );
      expect(find.text(archiveFirst), findsOneWidget);
      await tester.pageBack();
      await tap(tester, find.text('进行中'));
      await tap(tester, find.text(terminalObjective));
      await tap(tester, find.text('旁观终端'));
      final terminalView = find.byType(TerminalView);
      await visible(tester, terminalView);
      TerminalView current() => tester.widget<TerminalView>(terminalView);
      await waitFor(
        tester,
        () => current().terminal.buffer.getText().contains('MOBILE-PTY-READY'),
        'observer receives original remote PTY output',
      );
      expect(current().readOnly, isTrue);
      expect(current().terminal.onOutput, isNull);
      await tester.pageBack();
      await tap(tester, find.text('接管终端'));
      await visible(tester, terminalView);
      await waitFor(
        tester,
        () =>
            current().terminal.onOutput != null &&
            current().terminal.buffer.getText().contains('MOBILE-PTY-READY'),
        'interactive scoped relay connects',
      );
      expect(current().readOnly, isFalse);
      current().terminal.paste('mobile-input-proof');
      current().terminal.keyInput(TerminalKey.enter);
      await waitFor(
        tester,
        () => current().terminal.buffer.getText().contains(
          'MOBILE-INPUT-ACCEPTED',
        ),
        'executor acknowledged the exact input',
      );
      await tester.pageBack();
      await tester.pageBack();
      await tap(tester, find.byTooltip('切换工作空间'));
      await tap(tester, find.text('连接工作空间…'));
      await addProfile(tester, 'life', lifeUrl, lifeToken);
      await visible(tester, find.textContaining('personal-archive ·'));
      expect(find.textContaining('mobile-archive ·'), findsNothing);
      await tap(tester, find.textContaining('personal-archive ·'));
      await visible(tester, find.text(lifeTitle));
      await tester.pageBack();
    },
  );
}
