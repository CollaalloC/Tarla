import 'dart:convert';
import 'dart:io';
import 'package:flutter/services.dart';
import 'api.dart';

/// All profiles and the selected workspace are committed together.
class ProfileBook {
  final List<Profile> entries;
  final String? activeId;
  const ProfileBook([this.entries = const [], this.activeId]);

  Profile? get selected {
    for (final profile in entries) {
      if (profile.name == activeId) return profile;
    }
    return null;
  }

  ProfileBook select(Profile profile) => ProfileBook([
    for (final old in entries)
      if (old.name != profile.name) old,
    profile,
  ], profile.name);

  ProfileBook remove(String id) {
    final remaining = entries.where((p) => p.name != id).toList();
    return ProfileBook(
      remaining,
      activeId == id ? remaining.firstOrNull?.name : activeId,
    );
  }

  String encode() {
    Row endpoint(Endpoint e) => {'url': e.uri.toString(), 'token': e.token};
    return jsonEncode({
      'version': 1,
      'active': activeId,
      'profiles': [
        for (final p in entries)
          {
            'id': p.name,
            'supervisor': endpoint(p.supervisor),
            if (p.executor != null) 'executor': endpoint(p.executor!),
          },
      ],
    });
  }

  factory ProfileBook.decode(String raw) {
    Endpoint endpoint(dynamic value) {
      if (value is! Map ||
          value['url'] is! String ||
          value['token'] is! String) {
        throw const FormatException('本机配对信息损坏');
      }
      return Endpoint(value['url'] as String, value['token'] as String);
    }

    final data = jsonDecode(raw);
    if (data is! Map || data['version'] != 1 || data['profiles'] is! List) {
      throw const FormatException('本机配对信息格式错误');
    }
    final ids = <String>{};
    final profiles = <Profile>[];
    for (final p in data['profiles'] as List) {
      if (p is! Map || p['id'] is! String)
        throw const FormatException('工作空间 ID 格式错误');
      final id = p['id'] as String;
      if (!Profile.validId(id) || !ids.add(id))
        throw const FormatException('工作空间 ID 格式错误');
      profiles.add(
        Profile(
          id,
          endpoint(p['supervisor']),
          p['executor'] == null ? null : endpoint(p['executor']),
        ),
      );
    }
    final active = data['active'];
    if (profiles.isEmpty ? active != null : !ids.contains(active)) {
      throw const FormatException('找不到选中的工作空间');
    }
    return ProfileBook(profiles, active as String?);
  }
}

abstract class ProfileStore {
  Future<ProfileBook> load();
  Future<void> save(ProfileBook book);
}

class DeviceProfileStore implements ProfileStore {
  static const channel = MethodChannel('dev.tarla/paired-profiles');
  static ProfileStore? forPlatform() =>
      Platform.isAndroid || Platform.isIOS ? DeviceProfileStore() : null;

  @override
  Future<ProfileBook> load() async {
    final raw = await channel.invokeMethod<String>('read');
    return raw == null ? const ProfileBook() : ProfileBook.decode(raw);
  }

  @override
  Future<void> save(ProfileBook book) async {
    if (book.entries.isEmpty) {
      await channel.invokeMethod<void>('clear');
    } else {
      await channel.invokeMethod<void>('write', book.encode());
    }
  }
}
