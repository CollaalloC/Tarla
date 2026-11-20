#!/usr/bin/env bash
set -eu
gradle_home="$RUNNER_TEMP/tarla-gradle"
mkdir -p "$gradle_home/init.d"
cat > "$gradle_home/init.d/flutter-repositories.gradle" <<'GRADLE'
beforeSettings { settings ->
    if (settings.settingsDir.name == 'gradle' && settings.settingsDir.parentFile.name == 'flutter_tools') {
        settings.pluginManagement.repositories {
            mavenCentral()
            gradlePluginPortal()
        }
    }
}
GRADLE
echo "GRADLE_USER_HOME=$gradle_home" >> "$GITHUB_ENV"
