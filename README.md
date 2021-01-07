# chunker

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

### 1.4. array pointer

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

### 1.6. uid pointer

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
