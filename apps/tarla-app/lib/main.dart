import 'dart:async';
import 'package:flutter/material.dart';
import 'api.dart' as api;
import 'screens.dart';
import 'profiles.dart';
import 'local_core.dart';

void main() => runApp(const TarlaApp());

class TarlaApp extends StatelessWidget {
  final ProfileStore? profileStore;
  const TarlaApp({this.profileStore, super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'Tarla',
    theme: ThemeData(
      colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xff176756)),
    ),
    home: WorkspaceHome(store: profileStore),
    debugShowCheckedModeBanner: false,
  );
}

Future<api.Profile?> connectProfile(
  BuildContext context, [
  api.Profile? existing,
]) async {
  final fields = [
    existing?.name ?? '',
    existing?.supervisor.uri.toString() ?? '',
    existing?.supervisor.token ?? '',
    existing?.executor?.uri.toString() ?? '',
    existing?.executor?.token ?? '',
  ];
  String? error;
  bool addExecutor = existing?.executor != null;
  final result = await showDialog<api.Profile>(
    context: context,
    builder: (ctx) => StatefulBuilder(
      builder: (ctx, update) => AlertDialog(
        title: Text(existing == null ? '连接工作空间' : '编辑连接'),
        content: SizedBox(
          width: 420,
          child: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                for (final entry in [
                  (0, '工作空间 ID', 'work', false),
                  (1, '主管服务地址', 'https://core.example.com', false),
                  (2, '主管访问令牌', '', true),
                ])
                  TextFormField(
                    initialValue: fields[entry.$1],
                    readOnly: existing != null && entry.$1 == 0,
                    onChanged: (v) => fields[entry.$1] = v,
                    decoration: InputDecoration(
                      labelText: entry.$2,
                      hintText: entry.$3,
                    ),
                    obscureText: entry.$4,
                    autocorrect: false,
                    enableSuggestions: false,
                  ),
                SwitchListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('配置备用执行节点'),
                  value: addExecutor,
                  onChanged: (v) => update(() => addExecutor = v),
                ),
                if (addExecutor) ...[
                  TextFormField(
                    initialValue: fields[3],
                    autocorrect: false,
                    enableSuggestions: false,
                    onChanged: (v) => fields[3] = v,
                    decoration: const InputDecoration(labelText: '已配对执行节点地址'),
                  ),
                  TextFormField(
                    initialValue: fields[4],
                    autocorrect: false,
                    enableSuggestions: false,
                    onChanged: (v) => fields[4] = v,
                    obscureText: true,
                    decoration: const InputDecoration(labelText: '执行节点访问令牌'),
                  ),
                ],
                if (error != null)
                  Text(
                    error!,
                    style: TextStyle(color: Theme.of(ctx).colorScheme.error),
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
            onPressed: () {
              try {
                final name = fields[0].trim();
                if (!api.Profile.validId(name))
                  throw const FormatException('请输入服务端配置的工作空间 ID');
                Navigator.pop(
                  ctx,
                  api.Profile(
                    name,
                    api.Endpoint(fields[1], fields[2].trim()),
                    addExecutor
                        ? api.Endpoint(fields[3], fields[4].trim())
                        : null,
                  ),
                );
              } on FormatException catch (e) {
                update(() => error = e.message);
              }
            },
            child: Text(existing == null ? '连接' : '保存'),
          ),
        ],
      ),
    ),
  );
  return result;
}

class WorkspaceHome extends StatefulWidget {
  final ProfileStore? store;
  const WorkspaceHome({this.store, super.key});
  @override
  State<WorkspaceHome> createState() => _WorkspaceHomeState();
}

class _WorkspaceHomeState extends State<WorkspaceHome> {
  late final ProfileStore? store =
      widget.store ?? DeviceProfileStore.forPlatform();
  ProfileBook book = const ProfileBook();
  bool loading = false, saving = false;
  bool loadFailed = false;

  @override
  void initState() {
    super.initState();
    if (store != null) restore();
  }

  Future<void> restore() async {
    setState(() {
      loading = true;
      loadFailed = false;
    });
    try {
      final restored = await store!.load();
      if (mounted) setState(() => book = restored);
    } catch (_) {
      if (mounted) setState(() => loadFailed = true);
    } finally {
      if (mounted) setState(() => loading = false);
    }
  }

  Future<void> persist(ProfileBook next) async {
    if (saving) return;
    setState(() => saving = true);
    try {
      await store?.save(next);
      if (mounted)
        setState(() {
          book = next;
          loadFailed = false;
        });
    } catch (_) {
      if (mounted)
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('无法保存本机连接，请解锁设备后重试。')));
    } finally {
      if (mounted) setState(() => saving = false);
    }
  }

  Future<void> add([api.Profile? existing]) async {
    final profile = await connectProfile(context, existing);
    if (mounted && profile != null) await selectProfile(profile);
  }

  Future<void> selectProfile(api.Profile profile) =>
      persist(book.select(profile));

  Future<void> forget({bool all = false}) async {
    final id = book.activeId;
    if (!all && id == null) return;
    final yes = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: Text(all ? '清除本机连接？' : '移除 $id 的本机配对？'),
        content: const Text('本机保存的服务地址和访问令牌将被删除。'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, false),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(ctx, true),
            child: const Text('移除'),
          ),
        ],
      ),
    );
    if (mounted && yes == true)
      await persist(all ? const ProfileBook() : book.remove(id!));
  }

  @override
  Widget build(BuildContext context) {
    final selected = book.selected;
    final profiles = book.entries;
    if (loading || loadFailed) {
      return Scaffold(
        appBar: AppBar(title: const Text('Tarla')),
        body: Center(
          child: loading
              ? const CircularProgressIndicator()
              : Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    const Text('无法读取本机配对信息，请解锁设备后重试。'),
                    TextButton(
                      onPressed: saving ? null : restore,
                      child: const Text('重试'),
                    ),
                    TextButton(
                      onPressed: saving ? null : () => forget(all: true),
                      child: const Text('清除本机连接'),
                    ),
                  ],
                ),
        ),
      );
    }
    final header = AppBar(
      title: const Text('Tarla'),
      actions: [
        LocalCoreButton(onConnect: selectProfile),
        PopupMenuButton<int>(
          tooltip: '切换工作空间',
          enabled: !saving,
          onSelected: (i) {
            if (i == -1) {
              add();
            } else if (i == -2) {
              add(selected);
            } else if (i == -3) {
              forget();
            } else {
              persist(ProfileBook(profiles, profiles[i].name));
            }
          },
          itemBuilder: (_) => [
            for (var i = 0; i < profiles.length; i++)
              CheckedPopupMenuItem(
                value: i,
                checked: profiles[i] == selected,
                child: Text(profiles[i].name),
              ),
            const PopupMenuItem(value: -1, child: Text('连接工作空间…')),
            if (selected != null) ...[
              const PopupMenuItem(value: -2, child: Text('编辑当前连接…')),
              const PopupMenuItem(value: -3, child: Text('移除当前配对…')),
            ],
          ],
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: Row(
              children: [
                Text(selected?.name ?? '工作空间'),
                const Icon(Icons.expand_more),
              ],
            ),
          ),
        ),
      ],
    );
    if (selected == null) {
      return Scaffold(
        appBar: header,
        body: Center(
          child: FilledButton.icon(
            icon: const Icon(Icons.add_link),
            onPressed: saving ? null : () => add(),
            label: const Text('连接工作空间'),
          ),
        ),
      );
    }
    return Session(key: ObjectKey(selected), profile: selected, header: header);
  }
}

class Session extends StatefulWidget {
  final api.Profile profile;
  final PreferredSizeWidget header;
  const Session({required this.profile, required this.header, super.key});
  @override
  State<Session> createState() => _SessionState();
}

class _SessionState extends State<Session> {
  static const names = ['对话', '进行中', '等我处理', '记忆'];
  static const icons = [
    Icons.forum_outlined,
    Icons.play_circle_outline,
    Icons.inbox_outlined,
    Icons.bookmarks_outlined,
  ];
  final paths = ['conversations', 'runs', 'actions', 'memories'];
  late final api.TarlaApi supervisor = api.TarlaApi(
    widget.profile.supervisor,
    workspaceId: widget.profile.name,
  );
  late final api.TarlaApi? executor = widget.profile.executor == null
      ? null
      : api.TarlaApi(
          widget.profile.executor!,
          workspaceId: widget.profile.name,
        );
  int tab = 0;
  int generation = 0;
  bool busy = true;
  bool refreshing = false;
  bool direct = false;
  String? error;
  List<api.Row> data = [];
  List<api.Row> facts = [];
  Timer? timer;

  @override
  void initState() {
    super.initState();
    refresh();
    timer = Timer.periodic(const Duration(seconds: 8), (_) {
      if (!refreshing && ModalRoute.of(context)?.isCurrent == true)
        refresh(quiet: true);
    });
  }

  @override
  void dispose() {
    timer?.cancel();
    supervisor.close();
    executor?.close();
    super.dispose();
  }

  Future<void> refresh({bool quiet = false}) async {
    final stamp = ++generation;
    refreshing = true;
    if (!quiet) setState(() => busy = true);
    final currentTab = tab;
    try {
      var offline = false;
      List<api.Row> result;
      try {
        result = await supervisor.list([paths[currentTab]]);
      } catch (e) {
        if (executor == null || !api.unavailable(e)) {
          rethrow;
        }
        offline = true;
        result = await executor!.list([paths[currentTab]]);
      }
      final machine = currentTab == 3
          ? await (offline ? executor! : supervisor).list(['facts'])
          : <api.Row>[];
      if (mounted && stamp == generation)
        setState(() {
          direct = offline;
          data = result;
          facts = machine;
          error = null;
          busy = false;
        });
    } catch (e) {
      if (mounted && stamp == generation)
        setState(() {
          error = e.toString();
          busy = false;
        });
    } finally {
      if (stamp == generation) refreshing = false;
    }
  }

  void changeTab(int i) {
    if (tab == i) return;
    setState(() {
      tab = i;
      data = [];
      facts = [];
      error = null;
    });
    refresh();
  }

  @override
  Widget build(BuildContext context) {
    final client = direct ? executor! : supervisor;
    Future<void> openPage(Widget page) async {
      await Navigator.push(context, MaterialPageRoute(builder: (_) => page));
      if (mounted) await refresh();
    }

    Future<void> openRun(String id) =>
        openPage(RunPage(api: supervisor, executor: executor, id: id));

    void chat(String id) =>
        openPage(ChatPage(api: client, id: id, readOnly: direct));
    return Scaffold(
      appBar: widget.header,
      body: SafeArea(
        child: Center(
          child: ConstrainedBox(
            constraints: const BoxConstraints(maxWidth: 920),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                if (direct)
                  Material(
                    color: Theme.of(context).colorScheme.tertiaryContainer,
                    child: const Padding(
                      padding: EdgeInsets.all(12),
                      child: Text('已直连执行节点，主管离线。全局记忆写入与跨机派单已暂停。'),
                    ),
                  ),
                Padding(
                  padding: const EdgeInsets.fromLTRB(16, 12, 8, 8),
                  child: Row(
                    children: [
                      Expanded(
                        child: Text(
                          tab == 1 ? '进行中的任务' : names[tab],
                          style: Theme.of(context).textTheme.headlineSmall,
                        ),
                      ),
                      if (!direct && (tab == 0 || tab == 3))
                        IconButton(
                          tooltip: tab == 0 ? '新建对话' : '新增记忆',
                          icon: const Icon(Icons.add),
                          onPressed: () async {
                            if (tab == 0) {
                              chat(api.identifier());
                            } else {
                              await editMemory(context, client);
                              if (mounted) refresh();
                            }
                          },
                        ),
                      IconButton(
                        tooltip: '刷新',
                        icon: const Icon(Icons.refresh),
                        onPressed: refresh,
                      ),
                    ],
                  ),
                ),
                if (busy) const LinearProgressIndicator(),
                if (error != null)
                  Padding(
                    padding: const EdgeInsets.all(16),
                    child: SelectableText(error!),
                  ),
                Expanded(
                  child: ListView(
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                    children: [
                      if (!busy && error == null && data.isEmpty)
                        Padding(
                          padding: const EdgeInsets.all(18),
                          child: Text(
                            ['还没有对话', '还没有任务', '没有待处理事项', '还没有长期记忆'][tab],
                          ),
                        ),
                      for (final row in data)
                        switch (tab) {
                          0 => ListTile(
                            leading: const Icon(Icons.chat_bubble_outline),
                            title: Text(
                              '${row['title'] ?? row['conversation_id']}',
                            ),
                            subtitle: Text(
                              '${row['conversation_id']} · ${row['binding_mode'] ?? 'orchestrator'}',
                            ),
                            onTap: () => chat(row['conversation_id'] as String),
                          ),
                          1 => ListTile(
                            leading: const Icon(Icons.work_outline),
                            title: Text(
                              '${row['objective'] ?? row['receipt_summary'] ?? row['run_id']}',
                              maxLines: 2,
                              overflow: TextOverflow.ellipsis,
                            ),
                            subtitle: Text('${row['observed_state']}'),
                            onTap: () => openRun(row['run_id'] as String),
                          ),
                          2 => ListTile(
                            leading: const Icon(Icons.assignment_outlined),
                            title: Text('${row['title']}'),
                            subtitle: Text(
                              row['kind'] == 'terminal_input'
                                  ? '打开任务并接管终端'
                                  : '${row['kind']}',
                            ),
                            onTap: () async {
                              if (row['kind'] == 'terminal_input') {
                                await openRun(row['run_id'] as String);
                                return;
                              }
                              await handleAction(
                                context,
                                client,
                                row,
                                readOnly: direct,
                              );
                              if (mounted) refresh();
                            },
                          ),
                          _ => ListTile(
                            leading: const Icon(Icons.bookmark_outline),
                            title: Text('${row['content']}'),
                            subtitle: Text(
                              '${row['scope']} · v${row['version']} · 同步 #${row['sync_seq']}',
                            ),
                            onTap: direct
                                ? null
                                : () async {
                                    await editMemory(context, client, row: row);
                                    if (mounted) refresh();
                                  },
                          ),
                        },
                      if (tab == 3 && facts.isNotEmpty) ...[
                        const Divider(),
                        const ListTile(title: Text('设备环境')),
                        for (final f in facts)
                          ListTile(
                            title: Text('${f['fact_key']}'),
                            subtitle: SelectableText(
                              '${f['node_id']} · ${f['local_path']}\n${f['metadata_json']}',
                            ),
                          ),
                      ],
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
      bottomNavigationBar: NavigationBar(
        selectedIndex: tab,
        onDestinationSelected: changeTab,
        destinations: [
          for (var i = 0; i < 4; i++)
            NavigationDestination(icon: Icon(icons[i]), label: names[i]),
        ],
      ),
    );
  }
}
