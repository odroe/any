import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Redacted', () {
    test('supports constant construction and explicit access', () {
      const redacted = Redacted('secret');

      expect(redacted.reveal(), 'secret');
      expect(redacted.toString(), 'Redacted(<redacted>)');
    });

    test('preserves the original value without copying it', () {
      final value = <String>['secret'];
      final redacted = Redacted(value);

      expect(redacted.reveal(), same(value));
      value.add('another');
      expect(redacted.reveal(), ['secret', 'another']);
    });

    test('supports nullable values', () {
      const redacted = Redacted<String?>(null);

      expect(redacted.reveal(), isNull);
      expect(redacted.toString(), 'Redacted(<redacted>)');
    });

    test('formatting never calls the wrapped value toString', () {
      final redacted = Redacted(_Unformattable());

      expect('$redacted', 'Redacted(<redacted>)');
    });

    test('can be read through a wider type', () {
      const Redacted<num> redacted = Redacted<int>(7);

      final num value = redacted.reveal();
      expect(value, 7);
    });
  });
}

final class _Unformattable {
  @override
  String toString() => throw StateError('The secret must not be formatted.');
}
