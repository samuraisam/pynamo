Running the tests
=================

First, you will need an Amazon AWS account. If you don't have one of those, why
are you running these tests?

Create a file: `~/.pynamo.cfg` which provides the following data (filled in)::

    [aws]
    access_key_id = 
    secret_access_key = 

    [dynamodb]
    table_prefix = 

I use `nose` which pretty much rocks. From the main repository directy simply 
run `nosetests` which will pick up all the Pynamo tests and run them.
