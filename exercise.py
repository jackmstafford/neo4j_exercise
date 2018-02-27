from neo4j.v1 import GraphDatabase as gd
from sets import Set
from time import time

defaultNodeNames = ['zero', 'one']

def readFile(filename):
    try:
        with open(filename, 'r') as f:
            result = f.read()
    except:
        print('\nMake sure "{}" exists\n'.format(filename))
        raise
    return result

def readDataFile(filename):
    return readFile(filename).strip().split('\n')

def runQuery(query):
    with makeDriver() as driver:
        with driver.session() as session:
            with session.begin_transaction() as tx:
                q = tx.run(query)
    return q

def sendQueries(queries):
    with makeDriver() as driver:
        with driver.session() as session:
            with session.begin_transaction() as tx:
                for query in queries:
                    tx.run(query)

def deleteAll():
    runQuery('MATCH (n) DETACH DELETE n')

def makeDriver():
    uri = readFile('bolt.txt')
    password = readFile('password.txt')
    return gd.driver(uri, auth=('neo4j', password))

def setupFilenames(doMini=False):
    pre = ''
    if doMini:
        pre = 'mini_'
    filenames = ['{}m{}c.data'.format(pre, i) for i in ['v', 'o']]
    return filenames


def loadRealGraph():
    deleteAll()
    fileToQueries2(doMini=False, send=True)

def loadTestGraph():
    def makeIds(size, abc):
        ids = []
        for i in range(abc, abc + size):
            ids.append(chr(abc))
            abc += 1
        return ids
    deleteAll()
    data = {i + 'id': {} for i in ['c', 'o', 'r']}
    A = 65
    a = 97
    nc = 10
    no = 10
    nr = 15
    cids = makeIds(nc, A)
    oids = makeIds(no, A + nc)
    rids = makeIds(nr, a)
    for d, ids in zip(['cid', 'oid', 'rid'], [cids, oids, rids]):
        for i in ids:
            data[d][i] = Set()
    r = 4
    for i in range(r):
        data['rid'][rids[i]].add(cids[i])
    for i in range(r, r * 2):
        data['rid'][rids[i]].add(cids[i])
        data['rid'][rids[i]].add(cids[i - r])
    for i in range(5):
        data['oid'][oids[i]].add(cids[i])
        data['oid'][oids[i]].add(cids[i + 1])
        data['oid'][oids[i]].add(cids[i + 2])
    queries = makeQueries2(data)
    sendQueries(queries)
    return data, queries


def node(label, uid, name=''):
    return '({2}:{0} {{ uid: "{1}" }})'.format(label, uid, name)

def edge(label='EDGE', names=defaultNodeNames):
    return '({n0})-[:{label}]->({n1})'.format(n0=names[0], label=label, n1=names[1])

def mergeNode(label, uid):
    return 'MERGE {};'.format(node(label, uid))

def mergeTwoNodesAndEdge(labels, uids):
    nodes = [node(labels[i], uids[i], v) for i, v in enumerate(defaultNodeNames)]
    return 'MERGE {} MERGE {} MERGE {};'.format(nodes[0], nodes[1], edge())


def timeIt(defs, parameters, setup=''):
    times = []
    for d in defs:
        if len(setup) > 0:
            globals()[setup]()
        start = time()
        globals()[d](*parameters)
        end = time()
        times.append(end - start)
    return times

def averageTime(defs, parameters, num=100, setup=''):
    times = [0] * len(defs)
    for _ in range(num):
        for i, t in enumerate(timeIt(defs, parameters, setup)):
            times[i] += t
    for i in range(len(times)):
        times[i] /= num
    return times

def ftq(isFirst, doMini=False, send=True):
    filenames = setupFilenames(doMini)
    pre = ''
    if not isFirst:
        post = '2'
    data = globals()['readDataFiles' + post](filenames)
    queries = globals()['makeQueries' + post](data)
    if send:
        sendQueries(queries)
    return data, queries

def compareMethods(num=10):
    times = averageTime( ['fileToQueries' + v for v in ['', '2']],\
            [False, True], num, setup='deleteAll')
    print('Times: {}'.format(times))
    sup = 1
    if times[1] < times[0]:
        sup = 2
    print('Method {} is superior'.format(sup))


##########   METHOD ONE   ##########
# method one produces queries for each line in each data file

def readDataFiles(filenames):
    data = []
    for i, fil in enumerate(filenames):
        lines = readDataFile(fil)
        filData = []
        for line in lines:
            filData.extend([line.split()])
        data.append(filData)
    return data

def makeQueries(data):
    queries = []
    for d in data:
        ids = d[0]
        for line in d[1:]: # skip header
            # make a node for each id on each line
            # make an edge between the nodes
            queries.append(mergeTwoNodesAndEdge(ids, line[1:]))
    return queries

def fileToQueries(doMini=True, send=False):
    return ftq(True, doMini, send)


##########   METHOD TWO   ##########
# method two produces queries only for unique lines in each data file # method two is superior in terms of time (as can be seen via compareMethods) # this is due to the time it takes to send queries, so the fewer the better

def readDataFiles2(filenames):
    data = {}
    for fileIndex, fil in enumerate(filenames):
        lines = readDataFile(fil)
        ids = lines[0].split()
        for i in ids:
            if i not in data:
                data[i] = {}
        ids.insert(0, '') # make it the same len as the other lines
        for line in lines[1:]:
            line = line.split()
            if line[1] not in data[ids[1]]:
                data[ids[1]][line[1]] = Set()
            # making assumption based on having only these two files
            # which both only have edges going to cid and nothing else
            data[ids[1]][line[1]].add(line[2]) # only works for lines which split into three
    return data

def makeQueries2(data):
    queries = []
    for label in data:
        dl = data[label]
        for uid in dl:
            for u2 in dl[uid]:
                # make nodes and edges for uids in set
                # cid is assumption made during data formatting since same for given files
                queries.append(mergeTwoNodesAndEdge([label, 'cid'], [uid, u2]))
            if len(dl[uid]) == 0:
                queries.append(mergeNode(label, uid))
    return queries

def fileToQueries2(doMini=False, send=True):
    return ftq(False, doMini, send)


##########   QUESTIONS   ##########
#Please include the cypher queries you wrote to answer the following questions:

def q2():
    #2) How many rid nodes are there with no edges?
    name = 'C'
    query = '\
        MATCH (r:rid) \n\
        WHERE NOT (r)-[]-() \n\
        RETURN COUNT(r) AS {};'.format(name)
    print(query)
    c = runQuery(query).peek()[name]
    print('There are {} rid nodes with no edges'.format(c))
    return c

def q3():
    #3) How many cid nodes have more than one edge to an oid node?
    # assume any oid node, not a single oid node...
    # with my implementation there shouldn't ever be an oid with multiple edges to the same cid
    name = 'C'
    query = '\
        MATCH (c:cid)-[]-(:oid) \n\
        WITH c, COUNT(*) AS cnt \n\
        WHERE cnt > 1 \n\
        RETURN COUNT(c) AS {};'.format(name)
    print(query)
    c = runQuery(query).peek()[name]
    print('There are {} cid nodes with more than one edge to an oid node'.format(c))
    return c

def q4():
    #4) Are there any rid nodes with multiple cid nodes? If so, what percent?
    name, name2 = 'C', 'C1'
    query = '\
        MATCH (r:rid) \n\
        WITH COUNT(r) AS cr \n\
        MATCH (c:rid)-[]-(:cid) \n\
        WITH cr, c, COUNT(*) AS cnt \n\
        WHERE cnt > 1 \n\
        WITH cr, COUNT(c) AS cc \n\
        RETURN cc AS {}, 100.0 * cc / cr AS {};'.format(name, name2)
    print(query)
    c = runQuery(query).peek()
    cnt, perc = c[name], c[name2]
    print('There are {} rid nodes with multiple cid nodes, which is {}% of all rid nodes'\
            .format(cnt, perc))
    return cnt, perc
