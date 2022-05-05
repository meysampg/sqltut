SQLTut
======
![sqltut](https://user-images.githubusercontent.com/1416085/166968245-d3a15386-d88e-4f74-8f20-09a285ffe34d.gif)

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

 - BTree Internal Node Format
   ![internal node format](https://user-images.githubusercontent.com/1416085/166262436-cbd84aa7-64b6-4093-a541-9b456c2af575.png)

## TODO
 - Change structure to follow this architecture:

   ![arch2](https://user-images.githubusercontent.com/1416085/164418418-bc3abd64-246c-41bb-ba42-76b8e114d480.gif)