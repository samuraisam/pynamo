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

Running the full test suite takes a very long time. It has to create and destroy
many DynamoDB tables, and at the time of writing that seems to take quite a 
while.

If some tests fail, you may have to delete them from the AWS Console or another
command line client by hand.
