SQLTut
======
```shell
$ ./cmd -h
Usage of ./cmd:
  -db-path string
      Path of the DB file (default "./db")
  -engine string
      Engine to store and query (default "arraylike")
```

## Specs
 - BTree Leaf Node Format
   ![leaf node format](https://user-images.githubusercontent.com/1416085/165701217-0f15f412-add0-4e6c-aaff-8ce9e93a014d.png)

## TODO
 - Change structure to follow this architecture:

   ![arch2](https://user-images.githubusercontent.com/1416085/164418418-bc3abd64-246c-41bb-ba42-76b8e114d480.gif)