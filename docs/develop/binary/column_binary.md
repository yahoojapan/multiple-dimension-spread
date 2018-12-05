# Overview of column binary

Spread of MDS's components is represented by a collection of columns.
Column is an object that implements the IColumn interface on memory.
In order to write to a file, it is necessary to mutually convert each column to binary.

There is IColumnBinaryMaker as an interface for converting.
You can add your own implementation by implementing interconversion interface.

# Class with column binary
ColumnBinary is a class that expresses the data structure in memory as binary.
This class has arrays of not only byte arrays but also statistical information, class information used for conversion, compression, and child ColumnBinary class.

```
  public final String makerClassName;

  public final String compressorClassName;

  public final String columnName;
  public final ColumnType columnType;
  public final int rowCount;
  public final int rawDataSize;
  public final int logicalDataSize;
  public final int cardinality;

  public int binaryStart;
  public int binaryLength;
  public byte[] binary;

  public List<ColumnBinary> columnBinaryList;
```

| variable | summary |
| makerClassName  | Class name of IColumnBinaryMaker used for conversion  |
| compressorClassName | Class name used for compression  |
| columnName | Column name |
| columnType | Column type |
| rowCount | Number of elements in this column. |
| rawDataSize | Binary byte size. |
| logicalDataSize | Logical data size of this column. |
| cardinality | Cardinality. Not required. If it can not be calculated, -1. |
| binaryStart | Starting position of byte array |
| binaryLength | The length of the byte array |
| binary | Byte array converted column to binary |
| columnBinaryList | Child's ColumnBinary. It is used for Struct and Array columns. If there is no child, it is an empty List or Null. |

# Interface of IColumnBinaryMaker

```
public interface IColumnBinaryMaker{

  ColumnBinary toBinary( final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column ) throws IOException;

  IColumn toColumn( final ColumnBinary columnBinary ) throws IOException;

  int calcBinarySize( final IColumnAnalizeResult analizeResult );

  void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException;

  void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException;

}
```

## toBinary()

This method converts columns that are data structures on memory to binary.

## toColumn()

This method converts from binary to column.

* columnBinary is a binary that should have been converted in this class.

## calcBinarySize()
This method calculates binary uncompressed size from column statistics.
This data size is used to determine the encoding rule.

## loadInMemoryStorage()
This method loads directly into the data structure of another column type.
Assigning data is done through the interface of IMemoryAllocator.

## setBlockIndexNode
This function sets the index of Predicate pushdown.
Since it is not required, this function does nothing if it does not have an index.

# Example

## A simple example of column encoding

## A simple example of column decoding

## A simple example of in-memory loading

