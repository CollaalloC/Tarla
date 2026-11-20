import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/main.dart' as tarla;
import 'package:xterm/xterm.dart';

const workUrl = String.fromEnvironment('WORK_URL');
const workToken = String.fromEnvironment('WORK_TOKEN');
const workerUrl = String.fromEnvironment('WORKER_URL');
const workerToken = String.fromEnvironment('WORKER_TOKEN');
const lifeUrl = String.fromEnvironment('LIFE_URL');
const lifeToken = String.fromEnvironment('LIFE_TOKEN');
const lifeTitle = 'Personal archive from the life workspace';
const archiveFirst = 'Original mobile archive entry';
const directObjective = 'Mobile offline executor verification';
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

Future<void> back(WidgetTester tester) async {
  final route = tester
      .elementList(find.byType(Scaffold))
      .map(ModalRoute.of)
      .whereType<ModalRoute<dynamic>>()
      .lastWhere((route) => route.isCurrent);
  var removed = false;
  route.completed.then((_) => removed = true);
  await tester.pageBack();
  await waitFor(
      tester, () => removed, 'previous page fully removed from the navigator');
}

Future<void> selectTab(WidgetTester tester, String label) async {
  final bar = find.byType(NavigationBar);
  await visible(tester, bar);
  final destination = find.descendant(
    of: bar,
    matching: find.byWidgetPredicate(
        (w) => w is NavigationDestination && w.label == label),
  );
  final button = find
      .descendant(
        of: destination,
        matching:
            find.byWidgetPredicate((w) => w is InkResponse && w.onTap != null),
      )
      .hitTestable();
  await visible(tester, button);
  await tester.tap(button);
  await waitFor(tester, () {
    final nav = tester.widget<NavigationBar>(bar);
    return (nav.destinations[nav.selectedIndex] as NavigationDestination)
            .label ==
        label;
  }, 'selected navigation destination "$label"');
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
  String token, {
  bool executor = false,
}) async {
  expect(url, isNotEmpty);
  expect(token, isNotEmpty);
  tester.testTextInput.register();
  final fields = find.byType(TextFormField);
  await visible(tester, fields);
  if (executor) {
    expect(workerToken, isNotEmpty);
    expect(workerUrl, isNotEmpty);
    await tap(tester, find.text('配置备用执行节点'));
  }
  for (final entry in [
    id,
    url,
    token,
    if (executor) workerUrl,
    if (executor) workerToken
  ].indexed) {
    await tester.ensureVisible(fields.at(entry.$1));
    await tester.enterText(fields.at(entry.$1), entry.$2);
  }
  FocusManager.instance.primaryFocus?.unfocus();
  await tester.pump();
  tester.testTextInput.unregister();
  await tap(tester, find.widgetWithText(FilledButton, '连接'));
}

Future<void> verifyTerminal(WidgetTester tester,
    {required String ready,
    required String input,
    required String acknowledgment}) async {
  await visible(tester, find.text('任务 ID'));
  final runScroll = find
      .descendant(of: find.byType(ListView), matching: find.byType(Scrollable))
      .first;
  await tester.scrollUntilVisible(find.text('旁观终端'), 250,
      scrollable: runScroll, maxScrolls: 15);
  final keyboard = tester.testTextInput;
  keyboard.register();
  addTearDown(keyboard.unregister);
  await tap(tester, find.text('旁观终端'));
  final terminalView = find.byType(TerminalView);
  await visible(tester, terminalView);
  TerminalView current() => tester.widget<TerminalView>(terminalView);
  await waitFor(
    tester,
    () => current().terminal.buffer.getText().contains(ready),
    'observer receives original remote PTY output',
  );
  expect(current().readOnly, isTrue);
  expect(current().terminal.onOutput, isNull);
  await tester.tap(terminalView);
  await tester.pump();
  expect(keyboard.hasAnyClients, isFalse,
      reason: 'the observed terminal must reject keyboard input');
  await back(tester);
  await tap(tester, find.text('接管终端'));
  await visible(tester, terminalView);
  await waitFor(
    tester,
    () =>
        current().terminal.onOutput != null &&
        current().terminal.buffer.getText().contains(ready),
    'interactive scoped terminal connects',
  );
  expect(current().readOnly, isFalse);
  await tester.tap(terminalView);
  await waitFor(
      tester,
      () => keyboard.hasAnyClients && keyboard.editingState != null,
      'interactive terminal accepts platform text input');
  String editingText() =>
      TextEditingValue.fromJSON(keyboard.editingState!).text;
  keyboard.enterText('${editingText()}${input}x');
  await tester.pump();
  final baseline = editingText();
  expect(baseline, isNotEmpty, reason: 'software backspace must be available');
  keyboard.enterText(baseline.substring(0, baseline.length - 1));
  await tester.pump();
  await keyboard.receiveAction(TextInputAction.done);
  await waitFor(
    tester,
    () => current().terminal.buffer.getText().contains(
          acknowledgment,
        ),
    'executor acknowledged the corrected keyboard input',
  );
  FocusManager.instance.primaryFocus?.unfocus();
  await tester.pump();
  keyboard.unregister();
}
