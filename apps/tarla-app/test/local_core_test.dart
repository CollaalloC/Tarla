import 'dart:convert';
import 'dart:io';
import 'package:crypto/crypto.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tarla_app/local_core.dart';

void main() {
  final token = 'a' * 64;
  test(
    'proves local daemon identity before forming an authenticated profile',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      server.listen((request) {
        expect(request.headers.value(HttpHeaders.authorizationHeader), isNull);
        final nonce = request.uri.pathSegments.last;
        expect(nonce, matches(RegExp(r'^[0-9a-f]{64}$')));
        final proof = Hmac(
          sha256,
          utf8.encode(token),
        ).convert(utf8.encode('tarla-desktop-v1:$nonce'));
        request.response.write(jsonEncode({'proof': proof.toString()}));
        request.response.close();
      });
      final profile = await verifyLocalService(server.port, token);
      expect(profile.name, 'desktop-local');
      expect(profile.supervisor.token, token);
    },
  );
  test(
    'a stale port gets no bearer credential and fails identity verification',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      server.listen((request) {
        expect(request.headers.value(HttpHeaders.authorizationHeader), isNull);
        request.response.write('{"proof":"invalid"}');
        request.response.close();
      });
      await expectLater(
        verifyLocalService(server.port, token),
        throwsFormatException,
      );
    },
  );
}
