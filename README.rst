Features Supported
==================

DynamoDB supported features:

  * **Batched Operations** - the current DynamoDB API only supports batched
    fetch operations. Additionally DynamoDB makes it painful to fetch more than
    a few items at a time. Pynamo provides a useful batched operation facade.
  * **Set Operations** - DynamoDB has native support for sets of strings or
    numbers, which Pynamo supports with a much simplified API.
  * **UpdateItem** - Pynamo automatically uses `UpdateItem` when it makes sense.

Pynamo builds on the great `boto` package, whose underlying connection api
automatically uses a connection pool and supports keep-alive and timeouts.

Additionally, Pynamo comes with some features of it's own:
  
  * **Declaritive Schema** - Pynamo's schema goes beyond defining just a hash 
    key and range key. 
  * **Compound Keys** - keys that are composed of multiple keys.
  * **Synthesized Types** - attributes can be container types like `list`
    or `dict`
  * **Validated Attributes** - attributes are validated against the type chosen
    when writing persisted classes


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
