import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/main.dart';

void main() {
  testWidgets(
    'connect and switch workspaces through the header on a small screen',
    (tester) async {
      tester.view.physicalSize = const Size(375, 750);
      tester.view.devicePixelRatio = 1;
      addTearDown(tester.view.resetPhysicalSize);
      addTearDown(tester.view.resetDevicePixelRatio);
      await tester.pumpWidget(const TarlaApp());
      await tester.tap(find.text('连接工作空间'));
      await tester.pumpAndSettle();
      await tester.enterText(find.byType(TextField).at(0), '研发');
      await tester.enterText(
        find.byType(TextField).at(1),
        'https://example.test',
      );
      await tester.enterText(find.byType(TextField).at(2), 'test-only');
      await tester.tap(find.widgetWithText(FilledButton, '连接'));
      await tester.pumpAndSettle();
      expect(find.text('研发'), findsOneWidget);
      for (final name in ['对话', '进行中', '等我处理', '记忆']) {
        expect(
          find.descendant(
            of: find.byType(NavigationBar),
            matching: find.text(name),
          ),
          findsWidgets,
        );
      }
      await tester.tap(find.text('进行中').last);
      await tester.pumpAndSettle();
      expect(find.text('进行中的任务'), findsOneWidget);
      await tester.tap(find.byTooltip('切换工作空间'));
      await tester.pumpAndSettle();
      expect(find.text('连接工作空间…'), findsOneWidget);
      await tester.pumpWidget(const SizedBox.shrink());
    },
  );
}
