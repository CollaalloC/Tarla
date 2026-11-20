import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/api.dart';
import 'package:tarla_app/screens.dart';
import 'package:xterm/xterm.dart';

class _Socket extends Stream<dynamic> implements WebSocket {
  final incoming = StreamController<dynamic>();
  final bytes = <int>[];
  @override
  StreamSubscription<dynamic> listen(void Function(dynamic)? onData,
          {Function? onError, void Function()? onDone, bool? cancelOnError}) =>
      incoming.stream.listen(onData,
          onError: onError, onDone: onDone, cancelOnError: cancelOnError);
  @override
  void add(dynamic frame) {
    if (frame is List<int>) bytes.addAll(frame);
  }

  @override
  Future<void> close([int? code, String? reason]) => incoming.close();
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _AttachedApi extends TarlaApi {
  final _Socket socket;
  _AttachedApi(this.socket) : super(Endpoint('https://test.invalid', 'test'));
  @override
  Future<WebSocket> terminal(String id, String mode,
          {TarlaApi? offlineExecutor}) async =>
      socket;
}

void main() {
  testWidgets(
      'mobile IME commits UTF-8, deletes a character and submits to the attached PTY',
      (tester) async {
    final socket = _Socket();
    final client = _AttachedApi(socket);
    addTearDown(client.close);
    await tester.pumpWidget(MaterialApp(
        theme: ThemeData(platform: TargetPlatform.iOS),
        home: TerminalPage(api: client, id: 'r', mode: 'interactive')));
    await tester.pumpAndSettle();
    final view = find.byType(TerminalView);
    await tester.tap(view);
    await tester.pump(const Duration(milliseconds: 350));
    expect(tester.testTextInput.hasAnyClients, isTrue);
    String initial() =>
        TextEditingValue.fromJSON(tester.testTextInput.editingState!).text;
    Future<void> edit(String text) async {
      tester.testTextInput.updateEditingValue(TextEditingValue(
          text: text, selection: TextSelection.collapsed(offset: text.length)));
      await tester.pump();
    }

    await edit('${initial()}好');
    expect(socket.bytes, utf8.encode('好'));
    final base = initial();
    await edit(base.isEmpty ? '' : base.substring(0, base.length - 1));
    expect(socket.bytes.last, anyOf(8, 127),
        reason: 'software backspace must reach the remote PTY');
    await tester.testTextInput.receiveAction(TextInputAction.done);
    await tester.pump();
    expect(socket.bytes.last, 13);
    final prior = socket.bytes.length;
    await tester.tap(find.byTooltip('发送回车'));
    await tester.pump();
    expect(socket.bytes.sublist(prior), [13]);
    await tester.pumpWidget(const SizedBox.shrink());
  });
  testWidgets('observer renders split UTF-8 output and never sends input',
      (tester) async {
    final socket = _Socket();
    final client = _AttachedApi(socket);
    addTearDown(client.close);
    await tester.pumpWidget(MaterialApp(
        theme: ThemeData(platform: TargetPlatform.android),
        home: TerminalPage(api: client, id: 'r', mode: 'observe')));
    await tester.pumpAndSettle();
    final bytes = utf8.encode('远端');
    socket.incoming.add(bytes.sublist(0, 2));
    socket.incoming.add(bytes.sublist(2));
    await tester.pump();
    final view = tester.widget<TerminalView>(find.byType(TerminalView));
    expect(view.terminal.buffer.getText(), contains('远端'));
    expect(view.readOnly, isTrue);
    expect(find.byTooltip('发送回车'), findsNothing);
    view.terminal.paste('hidden input');
    expect(socket.bytes, isEmpty);
    await tester.pumpWidget(const SizedBox.shrink());
  });
}
