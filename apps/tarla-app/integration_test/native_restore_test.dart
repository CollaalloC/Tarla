import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

import 'native_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'a new app process restores the selected workspace and both secure credentials',
    (tester) async {
      // The host runner force-stops the first process. This phase receives no endpoint or tokens.
      await openApp(tester);
      await visible(tester, find.textContaining('personal-archive ·'));
      expect(find.textContaining('mobile-archive ·'), findsNothing);
      await tap(tester, find.byTooltip('切换工作空间'));
      await tap(tester, find.text('work'));
      await visible(tester, find.textContaining('mobile-archive ·'));
      expect(find.textContaining('personal-archive ·'), findsNothing);
    },
  );
}
