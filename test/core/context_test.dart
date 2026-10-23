import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('ServiceKey', () {
    test('binds values with their key', () {
      final count = ServiceKey<int>('count');
      final binding = count.bind(7);

      expect(binding.key, same(count));
      expect(binding.value, 7);
    });

    test('same names and types still identify different services', () {
      final first = ServiceKey<String>('database');
      final second = ServiceKey<String>('database');
      final context = Context.from([
        first.bind('primary'),
        second.bind('replica'),
      ]);

      expect(first, isNot(same(second)));
      expect(context.read(first), 'primary');
      expect(context.read(second), 'replica');
    });

    test('rejects invalid values through a widened key', () {
      final count = ServiceKey<int>('count');
      final ServiceKey<Object> widened = count;

      expect(() => widened.bind('invalid'), throwsA(isA<TypeError>()));
      final context = Context.from([widened.bind(7)]);
      expect(context.read(count), 7);
    });

    test('rejects invalid values through dynamic', () {
      final count = ServiceKey<int>('count');
      final dynamic untyped = count;

      expect(() => untyped.bind('invalid'), throwsA(isA<TypeError>()));
      expect(() => untyped.bind(null), throwsA(isA<TypeError>()));
    });
  });

  group('Context.from', () {
    test('accepts services with different types', () {
      final count = ServiceKey<int>('count');
      final label = ServiceKey<String>('label');
      final service = ServiceKey<Object>('service');
      final value = Object();
      final context = Context.from([
        count.bind(7),
        label.bind('example'),
        service.bind(value),
      ]);

      final int actualCount = context.read(count);
      final String actualLabel = context.read(label);
      expect(actualCount, 7);
      expect(actualLabel, 'example');
      expect(context.read(service), same(value));
    });

    test('copies the input bindings', () {
      final count = ServiceKey<int>('count');
      final bindings = [count.bind(7)];
      final context = Context.from(bindings);

      bindings[0] = count.bind(9);
      bindings.clear();

      expect(context.read(count), 7);
    });

    test('rejects duplicate keys, including identical bindings', () {
      final count = ServiceKey<int>('count');
      final binding = count.bind(7);

      expect(
        () => Context.from([binding, binding]),
        throwsA(
          isA<DuplicateService>()
              .having((error) => error.key, 'key', same(count))
              .having(
                (error) => error.toString(),
                'diagnostic',
                contains('count'),
              ),
        ),
      );
      expect(
        () => Context.from([binding, count.bind(9)]),
        throwsA(isA<DuplicateService>()),
      );
    });
  });

  group('read and find', () {
    test('distinguishes a bound null from an absent service', () {
      final present = ServiceKey<String?>('present');
      final absent = ServiceKey<String?>('absent');
      final context = Context.from([present.bind(null)]);

      expect(context.read(present), isNull);
      expect(
        context.find(present),
        isA<OptionSome<String?>>().having(
          (option) => option.value,
          'value',
          isNull,
        ),
      );
      expect(context.find(absent), isA<OptionNone<String?>>());
    });

    test('reports a missing service with its key and name', () {
      final missing = ServiceKey<int>('request count');
      final context = Context.from([]);

      expect(
        () => context.read(missing),
        throwsA(
          isA<MissingService>()
              .having((error) => error.key, 'key', same(missing))
              .having(
                (error) => error.toString(),
                'diagnostic',
                contains('request count'),
              ),
        ),
      );
      expect(context.find(missing), isA<OptionNone<int>>());
    });

    test('supports reading through a wider key type', () {
      final count = ServiceKey<int>('count');
      final context = Context.from([count.bind(7)]);
      final ServiceKey<num> widened = count;

      final num value = context.read(widened);
      final Option<num> found = context.find(widened);
      expect(value, 7);
      expect(
        found,
        isA<OptionSome<num>>().having((option) => option.value, 'value', 7),
      );
    });
  });

  group('merge', () {
    test('combines services without modifying either input', () {
      final count = ServiceKey<int>('count');
      final label = ServiceKey<String>('label');
      final first = Context.from([count.bind(7)]);
      final second = Context.from([label.bind('example')]);

      final merged = first.merge(second);

      expect(merged.read(count), 7);
      expect(merged.read(label), 'example');
      expect(first.find(label), isA<OptionNone<String>>());
      expect(second.find(count), isA<OptionNone<int>>());
    });

    test('rejects overlapping keys without changing either input', () {
      final count = ServiceKey<int>('count');
      final first = Context.from([count.bind(7)]);
      final second = Context.from([count.bind(9)]);

      expect(
        () => first.merge(second),
        throwsA(
          isA<DuplicateService>().having(
            (error) => error.key,
            'key',
            same(count),
          ),
        ),
      );
      expect(first.read(count), 7);
      expect(second.read(count), 9);
    });

    test('accepts an empty context on either side', () {
      final count = ServiceKey<int>('count');
      final original = Context.from([count.bind(7)]);
      final empty = Context.from([]);

      expect(original.merge(empty).read(count), 7);
      expect(empty.merge(original).read(count), 7);
    });
  });

  group('replace', () {
    test('changes only the named binding in a new context', () {
      final count = ServiceKey<int>('count');
      final other = ServiceKey<int>('other');
      final original = Context.from([count.bind(7), other.bind(11)]);

      final replaced = original.replace(count.bind(9));

      expect(replaced.read(count), 9);
      expect(replaced.read(other), 11);
      expect(original.read(count), 7);
    });

    test('can replace a bound null', () {
      final label = ServiceKey<String?>('label');
      final original = Context.from([label.bind(null)]);

      final replaced = original.replace(label.bind('example'));

      expect(replaced.read(label), 'example');
      expect(original.read(label), isNull);
    });

    test('rejects a missing key', () {
      final count = ServiceKey<int>('count');
      final context = Context.from([]);

      expect(
        () => context.replace(count.bind(7)),
        throwsA(
          isA<MissingService>().having(
            (error) => error.key,
            'key',
            same(count),
          ),
        ),
      );
      expect(context.find(count), isA<OptionNone<int>>());
    });
  });
}
