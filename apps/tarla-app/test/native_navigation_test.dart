import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/api.dart';
import 'package:tarla_app/main.dart';

import '../integration_test/native_support.dart' as native;

void main() {
  for (final platform in [TargetPlatform.iOS, TargetPlatform.android]) {
    testWidgets('switch to the task list after a $platform chat transition',
        (tester) async {
      tester.view.physicalSize = const Size(393, 852);
      tester.view.devicePixelRatio = 1;
      addTearDown(tester.view.resetPhysicalSize);
      addTearDown(tester.view.resetDevicePixelRatio);
      await tester.pumpWidget(MaterialApp(
        theme: ThemeData(platform: platform),
        home: Session(
          profile: Profile('work', Endpoint('https://example.test', 'fixture')),
          header: AppBar(title: const Text('work')),
        ),
      ));
      await tester.tap(find.byTooltip('新建对话'));
      await tester.pumpAndSettle();
      await native.back(tester);
      await native.selectTab(tester, '进行中');
      expect(find.text('进行中的任务'), findsOneWidget);
      await tester.pumpWidget(const SizedBox.shrink());
    });
  }
}
