import 'dart:async';

import 'package:any/any.dart';
import 'package:any/schema.dart';
import 'package:test/test.dart';

void main() {
  test('runtime checks preserve values and do not coerce input', () {
    final integer = Schema<int>.type();
    expect((integer.parse(3) as Success).value, 3);
    for (final input in [3.0, '3', true, null]) {
      final issue = (integer.parse(
        input,
      ) as Failure<int, ValidationError>).error.issues.single;
      expect(issue.code, 'type');
      expect(issue.message, 'Expected int.');
      expect(issue.path, isEmpty);
    }
    final object = Object();
    expect(
      (Schema<Object>.type().parse(object) as Success).value,
      same(object),
    );
    expect(Schema<Never>.type().parse(null), isA<Failure>());
    expect(Schema<Object?>.type().parse(null), isA<Success>());
    expect(
      (Schema<int>.type(name: 'integer').parse(
        'bad',
      ) as Failure<int, ValidationError>).error.issues.single.message,
      'Expected integer.',
    );
  });

  test('field combinations infer typed application records', () {
    final user = Schema<String>.type()
        .field('name')
        .zip(
          Schema<int>.type()
              .refine((age) => age >= 0, message: 'Must be nonnegative.')
              .field('age'),
        )
        .map((fields) => (name: fields.$1, age: fields.$2));
    final Result<({String name, int age}), ValidationError> result = user.parse(
      {'name': 'Ada', 'age': 37, 'extra': true},
    );
    expect((result as Success).value, (name: 'Ada', age: 37));
    final failure = user.parse({'name': 1, 'age': -1}) as Failure;
    final issues = (failure.error as ValidationError).issues;
    expect(issues.map((issue) => issue.path), [
      ['name'],
      ['age'],
    ]);
    expect(issues.map((issue) => issue.code), ['type', 'refinement']);
  });

  test('nested list and field paths accumulate in traversal order', () {
    final schema = Schema<int>.type().field('age').list().field('users');
    final result = schema.parse({
      'users': [
        {'age': 'bad'},
        <String, Object?>{},
        {'age': 4},
      ],
    }) as Failure;
    final issues = (result.error as ValidationError).issues;
    expect(issues.map((issue) => issue.path), [
      ['users', 0, 'age'],
      ['users', 1, 'age'],
    ]);
    expect(issues.map((issue) => issue.code), ['type', 'missing']);
  });

  test('missing fields remain different from explicit nullable values', () {
    final field = Schema<String>.type().nullable().field('name');
    expect((field.parse({'name': null}) as Success).value, isNull);
    expect((field.parse({'name': 'Ada'}) as Success).value, 'Ada');
    final issue = (field.parse(
      <String, Object?>{},
    ) as Failure<String?, ValidationError>).error.issues.single;
    expect(issue.code, 'missing');
    expect(issue.path, ['name']);
    expect(field.parse(null), isA<Failure>());
  });

  test('nullable bypasses its source only for null', () {
    var calls = 0;
    final schema = Schema<int>((input) {
      calls++;
      return Result.success(input as int);
    }).nullable();
    expect((schema.parse(null) as Success).value, isNull);
    expect(calls, 0);
    expect((schema.parse(1) as Success).value, 1);
    expect(calls, 1);
  });

  test(
    'list output is a typed immutable snapshot and empty input succeeds',
    () {
      final input = [1, 2];
      final schema = Schema<int>.type().list();
      final value =
          (schema.parse(input) as Success<List<int>, ValidationError>).value;
      input[0] = 3;
      expect(value, [1, 2]);
      expect(() => value.add(4), throwsUnsupportedError);
      expect((schema.parse([]) as Success).value, isEmpty);
      expect(schema.parse({'0': 1}), isA<Failure>());
    },
  );

  test('widened element schemas produce correctly typed lists', () {
    final Schema<num> numbers = Schema<int>.type();
    final schema = numbers.list().map(
      (values) => values.fold<num>(0, (sum, value) => sum + value),
    );
    expect((schema.parse([1, 2]) as Success).value, 3);
    expect(schema.parse([1, 2.5]), isA<Failure>());
  });

  test('refine and map skip invalid inputs and keep existing errors', () {
    final failure = ValidationError([
      ValidationIssue(code: 'custom', message: 'No.'),
    ]);
    final schema = Schema<int>((_) => Result.failure(failure))
        .refine((_) => fail('No predicate.'), message: 'Unused.')
        .map((_) => fail('No mapper.'));
    expect((schema.parse(1) as Failure).error, same(failure));
    final refined = Schema<int>.type().refine(
      (value) => value.isEven,
      message: 'Must be even.',
      code: 'even',
    );
    expect(
      (refined.parse(
        1,
      ) as Failure<int, ValidationError>).error.issues.single.code,
      'even',
    );
  });

  test('zip evaluates both schemas, preserving error order and duplicates', () {
    final issue = ValidationIssue(code: 'same', message: 'No.');
    final order = <String>[];
    Schema<int> build(String name) => Schema((_) {
      order.add(name);
      return Result.failure(ValidationError([issue]));
    });
    final result = build('first').zip(build('second')).parse(null) as Failure;
    expect(order, ['first', 'second']);
    expect((result.error as ValidationError).issues, [
      same(issue),
      same(issue),
    ]);
  });

  test('zip retains the failure when only one side fails', () {
    final first = Schema<String>.type().field('first');
    final second = Schema<int>.type().field('second');
    for (final input in [
      {'first': 'ok'},
      {'second': 1},
    ]) {
      final result = first.zip(second).parse(input) as Failure;
      expect((result.error as ValidationError).issues, hasLength(1));
    }
  });

  test('custom schemas are lazy and reusable without caching input', () {
    var calls = 0;
    final schema = Schema<int>((input) {
      calls++;
      final value = int.tryParse(input.toString());
      return value == null
          ? Result.failure(
              ValidationError([
                ValidationIssue(code: 'integer', message: 'Invalid integer.'),
              ]),
            )
          : Result.success(value);
    });
    expect(calls, 0);
    expect((schema.parse('42') as Success).value, 42);
    expect(schema.parse('bad'), isA<Failure>());
    expect(calls, 2);
  });

  test('custom callback throws propagate with the original stack trace', () {
    final error = Object();
    final trace = StackTrace.current;
    Never throwOriginal() => Error.throwWithStackTrace(error, trace);
    final schemas = [
      Schema<int>((_) => throwOriginal()),
      Schema<int>.type().map((_) => throwOriginal()),
      Schema<int>.type().refine((_) => throwOriginal(), message: 'Unused.'),
      Schema<int>((_) => throwOriginal())
          .zip(Schema<int>((_) => fail('No second parser.'))),
    ];
    for (final schema in schemas) {
      try {
        schema.parse(1);
        fail('Expected a throw.');
      } catch (caught, stack) {
        expect(caught, same(error));
        expect(stack, same(trace));
      }
    }
  });

  test('Future values returned by map remain values', () {
    final future = Future.value(1);
    final result = Schema<int>.type().map((_) => future).parse(1) as Success;
    expect(result.value, same(future));
  });

  test('validation composes with Task expected errors and defects', () async {
    final schema = Schema<int>.type();
    final task = Task<int, ValidationError, Object?>(
      (context) => Exit.fromResult(schema.parse(context.dependencies)),
    );
    expect((await task.run(3) as ExitSuccess).value, 3);
    final failure = (await task.run('3') as ExitFailure).cause as CauseFailure;
    expect(failure.error, isA<ValidationError>());
    final error = Object();
    final throwing = Schema<int>((_) => throw error);
    final exit = await Task<int, ValidationError, ()>(
      (_) => Exit.fromResult(throwing.parse(null)),
    ).run(()) as ExitFailure;
    expect((exit.cause as CauseDefect).error, same(error));
  });

  test('validation errors snapshot their paths and issue lists', () {
    final path = <Object>['users', 0];
    final issue = ValidationIssue(
      code: 'custom',
      message: 'Invalid.',
      path: path,
    );
    final issues = [issue];
    final error = ValidationError(issues);
    path.add('changed');
    issues.clear();
    expect(issue.path, ['users', 0]);
    expect(error.issues, [same(issue)]);
    expect(() => issue.path.add(1), throwsUnsupportedError);
    expect(() => error.issues.clear(), throwsUnsupportedError);
    expect(error.at('root').issues.single.path, ['root', 'users', 0]);
  });

  test('invalid diagnostic paths and empty failures are rejected', () {
    expect(() => ValidationError([]), throwsArgumentError);
    for (final segment in [Object(), -1, 1.5]) {
      expect(
        () => ValidationIssue(
          code: 'custom',
          message: 'Invalid.',
          path: [segment],
        ),
        throwsArgumentError,
      );
    }
  });
}
