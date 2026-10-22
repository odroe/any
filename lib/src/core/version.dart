const _defaultVersion = "0.0.0";
String _currentVersion = _defaultVersion;

/// Returns the current isolate's version string.
///
/// Returns `0.0.0` initially and after [resetCurrentVersion]. Otherwise,
/// returns the value most recently passed to [setCurrentVersion].
String getCurrentVersion() => _currentVersion;

/// Sets the current isolate's version string to [version].
///
/// The value is kept in memory and used as-is, without format validation.
void setCurrentVersion(String version) => _currentVersion = version;

/// Resets the current isolate's version string to `0.0.0`.
void resetCurrentVersion() => _currentVersion = _defaultVersion;
