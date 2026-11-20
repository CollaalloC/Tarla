import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'native_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets(
      'stored executor pairing takes over its PTY while the supervisor is stopped',
      (tester) async {
    await openApp(tester);
    await visible(tester, find.textContaining('已直连执行节点，主管离线'));
    expect(find.byTooltip('新建对话'), findsNothing);
    await selectTab(tester, '记忆');
    await visible(tester, find.textContaining('已直连执行节点，主管离线'));
    expect(find.byTooltip('新增记忆'), findsNothing);
    await selectTab(tester, '进行中');
    await tap(tester, find.text(directObjective));
    await visible(tester, find.text('主管离线，已直连执行节点'));
    await verifyTerminal(tester,
        ready: 'MOBILE-DIRECT-READY',
        input: 'offline-input-proof',
        acknowledgment: 'MOBILE-DIRECT-ACCEPTED');
  });
}
