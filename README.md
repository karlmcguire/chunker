# chunker

## 1: nquad
```
{
    "name": "alice"
}

  subject: "c.1"
predicate: "name"
 objectId:
objectVal: "alice"
   facets: []
```

## 2: nquad empty

```
{
    "friend": {}
}

<no nquads found>
```

## 3: nquad object

```
{
    "friend": {
        "name": "charlie"
    }
}

  subject: "c.1"
predicate: "friend"
 objectId: "c.2"
objectVal: 
   facets: []

  subject: "c.2"
predicate: "name"
 objectId: 
objectVal: "charlie"
   facets: []
```

### 4: nquad array objects

```
{
    "friend": [{
        "name": "charlie"
    }, {
        "name": "joshua"
    }]
}

  subject: "c.1"
predicate: "friend"
 objectId: "c.2"
objectVal:
   facets: []

  subject: "c.1"
predicate: "friend"
 objectId: "c.3"
objectVal: 
   facets: []

  subject: "c.2"
predicate: "name"
 objectId:
objectVal: "charlie"
   facets: []

  subject: "c.3"
predicate: "name"
 objectId: 
objectVal: "joshua"
   facets: []
```

### 5: nquad object uid

```
{
    "friend": {
        "uid": "1000",
        "name": "charlie"
    }
}

  subject: "c.1"
predicate: "friend"
 objectId: "1000"
objectVal:
   facets: []

  subject: "1000"
predicate: "name"
 objectId:
objectVal: "charlie"
   facets: []
```
