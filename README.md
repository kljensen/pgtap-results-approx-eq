# pgTAP approximately equal

I need a [pgTAP](https://pgtap.org/) test that is similar to 
`results_eq` but allows for some kind of tolerance when comparing
numeric values. E.g. I'd like to say if one row has `30.23223`
and another `30.23225` for some column `a` that those should
be consider equal.

This is my attempt to write such a function. You can see the
API in the `approx-equal.sql` file. It's like

```
SELECT pg_temp.results_approx_equal(
    $$select * from (values (1,50), (1, 10)) vals(a, b)$$,
    $$select * from (values (2,55), (0, 5)) vals(a, b)$$,
    json_build_object('a', 1, 'b', 5)
);
```

In that code block I'm saying "consider two rows to be equivalent
if their `a` columns differ by no more than `1` and their `b` 
columns differ by no more than `5`.

## License

[pgTap code](https://github.com/theory/pgtap) is Copyright (c) 2008-2020 David E. Wheeler.
Code I wrote and didn't copy is available under the [Unlicense](https://unlicense.org/).

