import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:xterm/xterm.dart';
import 'api.dart' as core;

void showFailure(BuildContext context, Object error) {
  ScaffoldMessenger.of(
    context,
  ).showSnackBar(SnackBar(content: Text(error.toString())));
}

Future<void> editMemory(
  BuildContext context,
  core.TarlaApi client, {
  core.Row? row,
}) async {
  String scope = row?['scope'] ?? 'global';
  String content = row?['content'] ?? '';
  final save = await showDialog<bool>(
    context: context,
    builder: (ctx) => AlertDialog(
      title: Text(row == null ? '新增记忆' : '修订记忆 · v${row['version']}'),
      content: SizedBox(
        width: 400,
        child: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextFormField(
                initialValue: scope,
                decoration: const InputDecoration(labelText: '范围'),
                onChanged: (s) => scope = s,
              ),
              TextFormField(
                initialValue: content,
                decoration: const InputDecoration(labelText: '记忆内容'),
                minLines: 3,
                maxLines: 8,
                onChanged: (s) => content = s,
              ),
            ],
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(ctx),
          child: const Text('取消'),
        ),
        FilledButton(
          onPressed: () => Navigator.pop(ctx, true),
          child: const Text('保存'),
        ),
      ],
    ),
  );
  if (save != true || !context.mounted) return;
  if (content.trim().isEmpty) {
    showFailure(context, '记忆内容不能为空');
    return;
  }
  try {
    await client.post(
      ['memories'],
      {
        'memory_id': row?['memory_id'] ?? core.identifier(),
        'scope': scope.trim(),
        'content': content,
        'base_version': row?['version'] ?? 0,
      },
    );
  } catch (e) {
    if (context.mounted) showFailure(context, e);
  }
}

Future<void> handleAction(
  BuildContext context,
  core.TarlaApi client,
  core.Row row, {
  required bool readOnly,
}) async {
  final conflict = row['kind'] == 'memory_conflict';
  final login = row['kind'] == 'auth_expired';
  final canResolve = row['kind'] == 'approval' || (conflict && !readOnly);
  final response = await showDialog<String>(
    context: context,
    builder: (ctx) => AlertDialog(
      title: Text('${row['title']}'),
      content: SingleChildScrollView(
        child: SelectableText(
          login
              ? '请在节点 ${row['node_id']} 上重新登录工位 ${row['slot_id']}。'
              : conflict
              ? '当前版本 v${row['current_version']}\n${row['content']}\n\n建议版本（基于 v${row['base_version']}）\n${row['proposed_text']}'
              : '任务 ${row['run_id']}\n${row['risk_reason'] ?? ''}',
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(ctx),
          child: const Text('关闭'),
        ),
        if (canResolve) ...[
          OutlinedButton(
            onPressed: () => Navigator.pop(ctx, 'rejected'),
            child: Text(conflict ? '保留当前版本' : '拒绝'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(ctx, 'approved'),
            child: Text(conflict ? '采用建议' : '批准'),
          ),
        ],
      ],
    ),
  );
  if (response == null) return;
  try {
    await client.post(
      ['actions', row['id'] as String],
      {
        'resolution': response,
        if (conflict) 'expected_version': row['current_version'],
      },
    );
  } catch (e) {
    if (context.mounted) showFailure(context, e);
  }
}

class ChatPage extends StatefulWidget {
  final core.TarlaApi api;
  final String id;
  final bool readOnly;
  const ChatPage({
    required this.api,
    required this.id,
    required this.readOnly,
    super.key,
  });
  @override
  State<ChatPage> createState() => _ChatPageState();
}

class _ChatPageState extends State<ChatPage> {
  final draft = TextEditingController();
  final scroll = ScrollController();
  List<core.Row> rows = [];
  String? error, pendingId, pendingText;
  bool sending = false;
  bool refreshing = false;
  bool loadingHistory = false;
  bool hasHistory = false;
  int generation = 0;
  Timer? timer;
  @override
  void initState() {
    super.initState();
    refresh();
    timer = Timer.periodic(const Duration(seconds: 4), (_) {
      if (!sending && !refreshing && ModalRoute.of(context)?.isCurrent == true)
        refresh();
    });
  }

  @override
  void dispose() {
    timer?.cancel();
    draft.dispose();
    scroll.dispose();
    super.dispose();
  }

  Future<void> refresh() async {
    final stamp = ++generation;
    refreshing = true;
    try {
      final initial = rows.isEmpty;
      final next = await widget.api.list(
        ['conversations', widget.id, 'messages'],
        query: initial
            ? {'latest': 'true', 'limit': '60'}
            : {'after_seq': '${rows.last['seq_no']}', 'limit': '200'},
      );
      if (!mounted || stamp != generation) return;
      final nearEnd = !scroll.hasClients || scroll.position.extentAfter < 120;
      final appended = next.isNotEmpty;
      setState(() {
        if (initial) hasHistory = next.length == 60;
        rows = [...rows, ...next];
        error = null;
      });
      if (nearEnd && appended)
        WidgetsBinding.instance.addPostFrameCallback((_) {
          if (mounted && scroll.hasClients)
            scroll.jumpTo(scroll.position.maxScrollExtent);
        });
    } catch (e) {
      if (mounted && stamp == generation)
        setState(
          () => error = e is core.ApiError && e.status == 404 && rows.isEmpty
              ? null
              : e.toString(),
        );
    } finally {
      if (stamp == generation) refreshing = false;
    }
  }

  Future<void> history() async {
    if (loadingHistory || rows.isEmpty) return;
    setState(() => loadingHistory = true);
    try {
      final previous = await widget.api.list(
        ['conversations', widget.id, 'messages'],
        query: {
          'latest': 'true',
          'limit': '60',
          'before_seq': '${rows.first['seq_no']}',
        },
      );
      if (!mounted) return;
      setState(() {
        rows = [...previous, ...rows];
        hasHistory = previous.length == 60;
      });
    } catch (e) {
      if (mounted) showFailure(context, e);
    } finally {
      if (mounted) setState(() => loadingHistory = false);
    }
  }

  Future<void> submit() async {
    final text = draft.text.trim();
    if (sending || text.isEmpty || widget.readOnly) return;
    if (pendingText != text) {
      pendingText = text;
      pendingId = core.identifier();
    }
    setState(() => sending = true);
    try {
      await widget.api.post(
        ['conversations', widget.id, 'messages'],
        {'text': text, 'source_msg_id': pendingId, 'channel': 'app'},
      );
      if (!mounted) return;
      draft.clear();
      pendingText = null;
      pendingId = null;
      await refresh();
    } catch (e) {
      if (mounted) showFailure(context, e);
    } finally {
      if (mounted) setState(() => sending = false);
    }
  }

  Future<void> bind() async {
    String node = '', slot = '', workdir = '';
    final mode = await showDialog<String>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('频道路由'),
        content: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              SelectableText('会话 ID · ${widget.id}'),
              TextField(
                decoration: const InputDecoration(labelText: '执行节点 ID'),
                onChanged: (v) => node = v,
              ),
              TextField(
                decoration: const InputDecoration(labelText: '工位 ID'),
                onChanged: (v) => slot = v,
              ),
              TextField(
                decoration: const InputDecoration(labelText: '工作目录'),
                onChanged: (v) => workdir = v,
              ),
            ],
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx),
            child: const Text('取消'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(ctx, 'orchestrator'),
            child: const Text('交给主管'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(ctx, 'pinned'),
            child: const Text('直通工位'),
          ),
        ],
      ),
    );
    if (mode == null) return;
    try {
      if (mode == 'pinned' &&
          [node, slot, workdir].any((v) => v.trim().isEmpty)) {
        throw const FormatException('请填写节点、工位和工作目录');
      }
      await widget.api.request(
        'PUT',
        ['conversations', widget.id, 'binding'],
        {
          'mode': mode,
          if (mode == 'pinned') ...{
            'node_id': node.trim(),
            'slot_id': slot.trim(),
            'workdir': workdir.trim(),
          },
        },
      );
      if (mounted)
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('频道路由已更新')));
    } catch (e) {
      if (mounted) showFailure(context, e);
    }
  }

  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(
      title: const Text('对话'),
      actions: [
        if (!widget.readOnly)
          IconButton(
            tooltip: '频道路由',
            icon: const Icon(Icons.alt_route),
            onPressed: bind,
          ),
      ],
    ),
    body: SafeArea(
      child: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 800),
          child: Column(
            children: [
              if (error != null)
                Padding(
                  padding: const EdgeInsets.all(12),
                  child: SelectableText(error!),
                ),
              if (hasHistory)
                TextButton(
                  onPressed: loadingHistory ? null : history,
                  child: const Text('加载更早的消息'),
                ),
              Expanded(
                child: ListView.builder(
                  controller: scroll,
                  padding: const EdgeInsets.all(16),
                  itemCount: rows.length,
                  itemBuilder: (ctx, i) {
                    final row = rows[i], user = row['sender_role'] == 'user';
                    return Align(
                      alignment: user
                          ? Alignment.centerRight
                          : Alignment.centerLeft,
                      child: Container(
                        constraints: const BoxConstraints(maxWidth: 600),
                        margin: const EdgeInsets.symmetric(vertical: 4),
                        padding: const EdgeInsets.all(12),
                        decoration: BoxDecoration(
                          color: user
                              ? Theme.of(ctx).colorScheme.primaryContainer
                              : Theme.of(ctx).colorScheme.surfaceContainer,
                          borderRadius: BorderRadius.circular(12),
                        ),
                        child: SelectableText('${row['content']}'),
                      ),
                    );
                  },
                ),
              ),
              if (!widget.readOnly)
                Padding(
                  padding: const EdgeInsets.all(12),
                  child: Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: draft,
                          enabled: !sending,
                          minLines: 1,
                          maxLines: 5,
                          decoration: const InputDecoration(
                            hintText: '给主管发消息',
                            border: OutlineInputBorder(),
                          ),
                        ),
                      ),
                      IconButton(
                        tooltip: '发送',
                        onPressed: sending ? null : submit,
                        icon: sending
                            ? const SizedBox.square(
                                dimension: 18,
                                child: CircularProgressIndicator(
                                  strokeWidth: 2,
                                ),
                              )
                            : const Icon(Icons.send_outlined),
                      ),
                    ],
                  ),
                ),
            ],
          ),
        ),
      ),
    ),
  );
}

class RunPage extends StatefulWidget {
  final core.TarlaApi api;
  final core.TarlaApi? executor;
  final String id;
  const RunPage({
    required this.api,
    required this.id,
    this.executor,
    super.key,
  });
  @override
  State<RunPage> createState() => _RunPageState();
}

class _RunPageState extends State<RunPage> {
  core.Row? row;
  late core.TarlaApi source = widget.api;
  String? error;
  bool loading = false;
  Timer? timer;
  @override
  void initState() {
    super.initState();
    refresh();
    timer = Timer.periodic(const Duration(seconds: 5), (_) {
      if (ModalRoute.of(context)?.isCurrent == true) refresh();
    });
  }

  @override
  void dispose() {
    timer?.cancel();
    super.dispose();
  }

  Future<void> refresh() async {
    if (loading) return;
    loading = true;
    try {
      var endpoint = widget.api;
      core.Row data;
      try {
        data = await endpoint.get(['runs', widget.id]);
      } catch (e) {
        if (widget.executor == null || !core.unavailable(e)) rethrow;
        endpoint = widget.executor!;
        data = await endpoint.get(['runs', widget.id]);
      }
      if (mounted)
        setState(() {
          source = endpoint;
          row = data;
          error = null;
        });
    } catch (e) {
      if (mounted) setState(() => error = e.toString());
    } finally {
      loading = false;
    }
  }

  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('任务详情')),
    body: ListView(
      padding: const EdgeInsets.all(20),
      children: [
        if (source != widget.api)
          const ListTile(
            leading: Icon(Icons.lan_outlined),
            title: Text('主管离线，已直连执行节点'),
          ),
        if (error != null) SelectableText(error!),
        if (row == null && error == null) const LinearProgressIndicator(),
        if (row != null) ...[
          for (final field in {
            'run_id': '任务 ID',
            'observed_state': '执行状态',
            'target_node_id': '执行节点',
            'target_slot_id': '执行工位',
            'receipt_summary': '结果回执',
            'artifacts_json': '产物',
          }.entries)
            if (row![field.key] != null)
              ListTile(
                title: Text(field.value),
                subtitle: SelectableText('${row![field.key]}'),
              ),
          OutlinedButton.icon(
            icon: const Icon(Icons.subject),
            label: const Text('查看执行日志'),
            onPressed: () => Navigator.push(
              context,
              MaterialPageRoute(
                builder: (_) => LogPage(client: source, id: widget.id),
              ),
            ),
          ),
          if (row!['hc_terminal_id'] != null)
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                for (final mode in ['observe', 'interactive'])
                  OutlinedButton.icon(
                    icon: const Icon(Icons.terminal),
                    onPressed:
                        row!['terminal_modes'] is List &&
                            !(row!['terminal_modes'] as List).contains(mode)
                        ? null
                        : () {
                            Navigator.push(
                              context,
                              MaterialPageRoute(
                                builder: (_) => TerminalPage(
                                  api: source,
                                  offlineExecutor: identical(source, widget.api)
                                      ? widget.executor
                                      : null,
                                  id: widget.id,
                                  mode: mode,
                                ),
                              ),
                            );
                          },
                    label: Text(mode == 'observe' ? '旁观终端' : '接管终端'),
                  ),
              ],
            ),
          if (row!['terminal_input_hint'] is String &&
              (row!['terminal_input_hint'] as String).trim().isNotEmpty)
            Padding(
              padding: const EdgeInsets.only(top: 8),
              child: Text(row!['terminal_input_hint'] as String),
            ),
        ],
      ],
    ),
  );
}

class LogPage extends StatefulWidget {
  final core.TarlaApi client;
  final String id;
  const LogPage({required this.client, required this.id, super.key});
  @override
  State<LogPage> createState() => _LogPageState();
}

class _LogPageState extends State<LogPage> {
  List<String> lines = [];
  int offset = 0;
  bool eof = false, loading = false;
  String? error;
  @override
  void initState() {
    super.initState();
    load();
  }

  Future<void> load() async {
    if (loading) return;
    setState(() => loading = true);
    try {
      final page = await widget.client.get(
        ['runs', widget.id, 'log'],
        query: {'offset': '$offset', 'limit': '40'},
      );
      final batch = (page['lines'] as List).cast<String>();
      if (!mounted) return;
      setState(() {
        lines = batch;
        offset = (page['next_offset'] as int?) ?? offset + batch.length;
        eof = (page['eof'] as bool?) ?? batch.length < 40;
        error = null;
      });
    } catch (e) {
      if (mounted) setState(() => error = e.toString());
    } finally {
      if (mounted) setState(() => loading = false);
    }
  }

  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('执行日志')),
    body: SafeArea(
      child: Column(
        children: [
          if (loading) const LinearProgressIndicator(),
          if (error != null) SelectableText(error!),
          Expanded(
            child: ListView(
              padding: const EdgeInsets.all(16),
              children: [
                SelectableText(
                  lines.join('\n'),
                  style: const TextStyle(fontFamily: 'monospace'),
                ),
              ],
            ),
          ),
          TextButton(
            onPressed: (loading || eof) ? null : load,
            child: Text(eof ? '已到页尾' : '下一页'),
          ),
        ],
      ),
    ),
  );
}

class TerminalPage extends StatefulWidget {
  final core.TarlaApi api;
  final core.TarlaApi? offlineExecutor;
  final String id, mode;
  const TerminalPage({
    required this.api,
    required this.id,
    required this.mode,
    this.offlineExecutor,
    super.key,
  });
  @override
  State<TerminalPage> createState() => _TerminalPageState();
}

class _TerminalPageState extends State<TerminalPage> {
  final terminal = Terminal(maxLines: 3000);
  WebSocket? socket;
  StreamSubscription<String>? output;
  String status = '正在申请接管票据…';
  @override
  void initState() {
    super.initState();
    connect();
  }

  Future<void> connect() async {
    try {
      final ws = await widget.api.terminal(
        widget.id,
        widget.mode,
        offlineExecutor: widget.offlineExecutor,
      );
      if (!mounted) {
        await ws.close();
        return;
      }
      socket = ws;
      setState(() => status = '');
      if (widget.mode == 'interactive') {
        terminal.onOutput = (data) => ws.add(utf8.encode(data));
        terminal.onResize = (cols, rows, px, py) =>
            ws.add(jsonEncode({'cols': cols, 'rows': rows}));
        ws.add(
          jsonEncode({'cols': terminal.viewWidth, 'rows': terminal.viewHeight}),
        );
      }
      // A UTF-8 character may span any number of PTY frames.
      output = ws
          .where((frame) => frame is List<int>)
          .cast<List<int>>()
          .transform(const Utf8Decoder(allowMalformed: true))
          .listen(
            terminal.write,
            onError: (Object e) {
              if (mounted) setState(() => status = e.toString());
            },
            onDone: () {
              if (mounted) setState(() => status = '终端连接已关闭');
            },
          );
    } catch (e) {
      if (mounted) setState(() => status = e.toString());
    }
  }

  @override
  void dispose() {
    terminal.onOutput = null;
    terminal.onResize = null;
    output?.cancel();
    socket?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: Text(widget.mode == 'observe' ? '旁观终端' : '接管终端')),
    body: SafeArea(
      child: Column(
        children: [
          if (status.isNotEmpty)
            Padding(
              padding: const EdgeInsets.all(8),
              child: SelectableText(status),
            ),
          Expanded(
            child: TerminalView(
              terminal,
              readOnly: widget.mode == 'observe',
              autofocus: widget.mode == 'interactive',
            ),
          ),
        ],
      ),
    ),
  );
}
