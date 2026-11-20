import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/main.dart' as tarla;

const workUrl = String.fromEnvironment('WORK_URL');
const workToken = String.fromEnvironment('WORK_TOKEN');
const lifeUrl = String.fromEnvironment('LIFE_URL');
const lifeToken = String.fromEnvironment('LIFE_TOKEN');
const lifeTitle = 'Personal archive from the life workspace';
const archiveFirst = 'Original mobile archive entry';
const terminalObjective = 'Mobile terminal relay verification';

Future<void> waitFor(
  WidgetTester tester,
  bool Function() ready,
  String description, {
  Duration timeout = const Duration(seconds: 35),
}) async {
  final end = DateTime.now().add(timeout);
  while (!ready() && DateTime.now().isBefore(end)) {
    await tester.pump(const Duration(milliseconds: 100));
  }
  expect(ready(), isTrue, reason: description);
}

Future<void> visible(WidgetTester tester, Finder finder) =>
    waitFor(tester, () => finder.evaluate().isNotEmpty, finder.toString());

Future<void> tap(WidgetTester tester, Finder finder) async {
  await visible(tester, finder);
  await tester.ensureVisible(finder.first);
  await tester.tap(finder.first);
  await tester.pump();
  await tester.pump(const Duration(milliseconds: 350));
}

Future<void> openApp(WidgetTester tester) async {
  expect(Platform.isIOS || Platform.isAndroid, isTrue);
  tarla.main();
  await tester.pump();
}

Future<void> addProfile(
  WidgetTester tester,
  String id,
  String url,
  String token,
) async {
  expect(url, isNotEmpty);
  expect(token, isNotEmpty);
  tester.testTextInput.register();
  final fields = find.byType(TextFormField);
  await visible(tester, fields);
  for (final entry in [id, url, token].indexed) {
    await tester.ensureVisible(fields.at(entry.$1));
    await tester.enterText(fields.at(entry.$1), entry.$2);
  }
  FocusManager.instance.primaryFocus?.unfocus();
  tester.testTextInput.unregister();
  await tester.pump();
  await tap(tester, find.widgetWithText(FilledButton, '连接'));
}
