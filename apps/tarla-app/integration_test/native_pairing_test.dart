import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'native_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'two isolated workspaces, archived history and a scoped remote PTY',
    (tester) async {
      await openApp(tester);
      await tap(tester, find.text('连接工作空间'));
      await addProfile(tester, 'work', workUrl, workToken, executor: true);
      await visible(tester, find.textContaining('mobile-archive ·'));
      await tap(tester, find.textContaining('mobile-archive ·'));
      await tap(tester, find.text('加载更早的消息'));
      await waitFor(tester, () => find.text('加载更早的消息').evaluate().isEmpty,
          'prior archive page loaded');
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
      await back(tester);
      await selectTab(tester, '进行中');
      await tap(tester, find.text(terminalObjective));
      await verifyTerminal(tester,
          ready: 'MOBILE-PTY-READY',
          input: 'mobile-input-proof',
          acknowledgment: 'MOBILE-INPUT-ACCEPTED');
      await back(tester);
      await back(tester);
      await tap(tester, find.byTooltip('切换工作空间'));
      await tap(tester, find.text('连接工作空间…'));
      await addProfile(tester, 'life', lifeUrl, lifeToken);
      await visible(tester, find.textContaining('personal-archive ·'));
      expect(find.textContaining('mobile-archive ·'), findsNothing);
      await tap(tester, find.textContaining('personal-archive ·'));
      await visible(tester, find.text(lifeTitle));
      await back(tester);
    },
  );
}
