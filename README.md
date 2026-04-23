# OCgit
very basic git implementation for OpenComputers

This uses a lot of energy due to the use of inflate on very large data sizes.

I would advise on setting the `dataCardComplex` and `dataCardComplexByte` values to 0 to eliminate energy usage of the data card on the complex inflate calls it makes.