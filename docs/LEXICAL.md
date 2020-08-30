# Lexical Literal Storage

This is a proposal for altering the way we store values. At present, all values are stored as a quoted string with a type suffix, for example `"42"^xsd:integer`. This kind of storage has quite a low information density. the number '42' can be stored in 6 bits, but doing it this way actually costs us 16 bytes, or 128 bits.

Another problem is that numbers are not stored in numerical order, but instead in lexicographical order. This means that 100 is stored before 42, because lexicographically, `"100"^xsd:integer` is less than `"42"^xsd:integer`. This makes range queries hard to do.

Finally, it is hard to retrieve by type. We're often only interested in values of a certain type, such as dates, but because everything is stored lexicographic with the type at the end, values of different types are stored interspersed.

This proposal attempts to resolve all these issues at once by modifying the way we store our values. 

# Format changes

The proposal has a few different parts.

## Replace the type suffix with a type byte

We intend to support the full range of xsd/xdd types, but even that full range is still a relatively small number that fits into a single byte. Therefore, we can replace the type suffix string with a single byte.

## Make the type a prefix rather than a suffix

By moving the type to the front, we ensure that in lexical ordering, all values of the same type in the same layer are stored adjacently. We can then easily limit searches to a particular type by determining the id ranges for this type in the various layers.

## Store data types in a way that makes lexical order the same as logical order

Many data types have some kind of representation that makes lexical order and logical order the same. The exact way to do this will be different for different data types. I'll just consider a subset of types here.

### Integers

Since many types can be mapped onto the integers, integers is the most reasonable case to consider and solve first.

**Negative numbers**

A troublesome feature of integers is that they can be both positive and negative. The standard binary representation of signed integers, two's complement, is not lexicographical. These numbers need to be converted to a format which is lexicographical. 

luckily there's an easy solution. For any fixed-width negative integer, we can simply add an offset to turn any signed integer into an unsigned integer. For example, the range (-8..-1) (all 3-bit negative integers) can easily be turned into the range (0..7) by adding 8. The positive integers are already lexicographical.

We'll need to store the sign bit separately for this to work, but we can do so inside the type byte.

**variable-width numbers**

Unfortunately we cannot tell in advance how large our integers will be. Any upper limit we pick is going to be arbitrary, and there will always be use cases for even larger numbers.

It is not hard to store numbers of arbitrary size. We can just use as many bytes as needed, in big-endian format, to fit the whole number. However, 0x100, while a greater number than 0xff, comes first in lexicographical order. We need a scheme which ensures that 0xff is always stored in a way which'll be seen as lexicographically less than 0x100.

A possible way to do this is by prepending every number by the amount of bytes used to store it. This'd store 0xff and 0x100 as 0x1ff and 0x2100 . But this is still not truly variable width, as that would limit the maximum amount of bytes in an integer to 255. That lets us store some pretty huge numbers, but it is still a limit.

To get around that too, we can store the number length much like how we store prefix lengths in our dictionary already (the vbyte mechanism): we make the most significant bit signify whether this is the last byte in the length. If it is 0, this is the last byte, if it is 1, more bytes are to follow.

There is one more problem to this though. Such size vbytes have a lexicographical order from small to large. This is correct for positive numbers, but it's the wrong way around for negative numbers. Therefore we actually need two vbyte schemes:

- for positive numbers: store number size as a vbyte using 1 as continuation bit and 0 as final bit.
- for negative numbers: invert the number size, turning all 1-bit into 0-bits. Then store it as a vbyte using 0 as continuation bit and 1 as final bit.

This ensures proper ordering of both positive and negative numbers.

**Putting it all together**

So in total, an integer is to be stored as:

sign-bit + size vbyte + big endian number

Where

- sign-bit is 0 for negative and 1 for positive, and is going to be the last bit of the type byte.
- size vbyte storage format is dependent the number being positive or negative.
- big endian number is either the big endian positive number or the offsetted big endian negative number.

### date, DateTime

DateTime can easily be mapped onto integers by converting to unix timestamp, with negative timestamps representing dates before 1970-01-01.

### Decimals

Decimals can be converted to an integer by removing the point. We can then prepend a vbyte representing the amount of (base-10) digits before the point, resulting in a lexicographically ordered decimal representation.

### base64binary, hexbinary

Rather than storing this as base64 or hex, we could actually store it as a binary, prefixed by (vbyte) length.

### Other types

We're unlikely to be able to convert every data type to a dense and quickly queryable ranged version in a reasonable amount of time. For now, we can leave any type we don't immediately care about as a string, just like it is stored currently. With the new format we can actually leave out the quotation marks and save a whopping 2 bytes for every value.

# API changes

We'll need new api bits to constrain triple lookups. Every triple retrieval function should be expanded with 4 optional arguments:

- value type
- range start
- range end
- range type: (), [), (], []

# Things to consider

## Canonical form

xsd types often allow multiple representations for the same value. Currently these values are saved as different strings, and querying for one will not retrieve the other. When we start storing them differently, we'll probably lose out on the exact lexical form in favor of a canonical form. Is this a problem? Arguably, that's a feature.

## Accumulation of stale dictionary entries

This is actually already a problem, but people expect to be able to change numerical data cheaply. Unfortunately, due to our append-only nature, each numerical change will add a new value to the dictionary, without removing the old one. This results in increasingly large dictionaries, long dictionary ids, and slower query times (as each number add will need to query among the known numbers in all layers to see if it doesn't exist yet).

If a database is written to often, it probably will need to be regularly rebuilt, dropping the history.

## DB conversion

We've had a stable database format for a few months now. It looks like that's going to change now. Unfortunately we don't really have a db conversion plan yet. We'll need to create a tool to do the conversion, and introduce db version numbers that can be checked to ensure we don't reconvert an already converted database.

## Custom data types

xsd allows data type composition. Do we want to support that in some form? Do we need to to be standards compliant? Does that even matter?

XDD range types

xdd:integerRange, xdd:decimalRange, xsd:dateRange - need some consideration with respect to indexing...
