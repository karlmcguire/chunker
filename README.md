# chunker

* [1. nquad](#1-nquad)
    + [1.1. basic](#11-basic)
    + [1.2. empty](#12-empty)
    + [1.3. object pointer](#13-object-pointer)
    + [1.4. array](#14-array)
        - [1.4.1. array pointer](#141-array-pointer)
    + [1.5. uid](#15-uid)
        - [1.5.1. uid pointer](#151-uid-pointer)
* [2. facet](#2-facet)
    + [2.1. scalar](#21-scalar)
        - [2.1.1 scalar array pointer](#211-scalar-array-pointer)
    + [2.2. map](#22-map)

## 1. nquad

### 1.1. basic

```json
{
    "name": "alice"
}
```

```
  subject: "c.1"
predicate: "name"
 objectId: 
objectVal: "alice"
   facets:
```

### 1.2. empty

```json
{
    "name": null
}
```

```json
{
    "name": {}
}
```

```json
{
    "name": []
}
```

```
(no nquads found)
```

### 1.3. object pointer

```json
{
    "friend": {
        "name": "charlie"
    }
}
```

```
  subject: "c.1"
predicate: "friend"
 objectId: "c.2"
objectVal:
   facets:

  subject: "c.2"
predicate: "name"
 objectId: 
objectVal: "charlie"
   facets:
```

### 1.4. array

```json
{
    "friend": ["charlie", "bob"]
}
```

```
  subject: "c.1"
predicate: "friend"
 objectId:
objectVal: "charlie"
   facets:
  
  subject: "c.1"
predicate: "friend"
 objectId: 
objectVal: "bob"
   facets:

```

#### 1.4.1. array pointer

```json
{
    "friend": [
        {
            "name": "charlie"
        },
        {
            "name": "bob"
        }
    ]
}
```

```
  subject: "c.1"
predicate: "friend"
 objectId: "c.2"
objectVal:
   facets:

  subject: "c.1"
predicate: "friend"
 objectId: "c.3"
objectVal:
   facets:
  
  subject: "c.2"
predicate: "name"
 objectId: "charlie"
objectVal:
   facets:
  
  subject: "c.3"
predicate: "name"
 objectId: "bob"
objectVal:
   facets:
```

### 1.5. uid

```json
{
    "uid": "1000",
    "name": "charlie"
}
```

```
  subject: "1000"
predicate: "name"
 objectId: 
objectVal: "charlie"
   facets:
```

#### 1.5.1. uid pointer

```json
{
    "friend": {
        "uid": "1000",
        "name": "charlie"
    }
}
```

```
  subject: "c.1"
predicate: "friend"
 objectId: "1000"
objectVal:
   facets:
  
  subject: "1000"
predicate: "name"
 objectId: 
objectVal: "charlie"
   facets:
```

## 2. facet

### 2.1. scalar

```json
{
    "friend": "charlie",
    "friend|close": true
}
```

```
  subject: "c.1"
predicate: "friend"
 objectId: 
objectVal: "charlie"
   facets: [
        key: "close",
        val: []byte{0x01},
    valType: bool
   ]
```

### 2.1.1 scalar array pointer

```json
{
    "name": "alice",
    "friend": [
        {
            "name": "charlie",
            "friend|close": true
        }
    ]
}
```

```
  subject: "c.1"
predicate: "name"
 objectId: 
objectVal: "alice"
   facets: 
  
  subject: "c.2"
predicate: "name"
 objectId: 
objectVal: "charlie"
   facets: 

  subject: "c.1"
predicate: "friend"
 objectId: "c.2"
objectVal:
   facets: [
        key: "close",
        val: []byte{0x01},
    valType: bool
   ]
```

### 2.2. map

```json
{
    "friend": ["charlie", "bob", "josh"],
    "friend|from": {
        "0": "college",
        "2": "work"
    }
}
```

```
  subject: "c.1"
predicate: "friend"
 objectId: 
objectVal: "charlie"
   facets: [
        key: "from",
        val: []byte("college"),
    valType: string
   ]
  
  subject: "c.1"
predicate: "friend"
 objectId: 
objectVal: "bob"
   facets: 
  
  subject: "c.1"
predicate: "friend"
 objectId: 
objectVal: "josh"
   facets: [
        key: "from",
        val: []byte("work"),
    valType: string
   ]
```
