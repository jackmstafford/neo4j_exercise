from neo4j.v1 import GraphDatabase as gd
from sets import Set
from time import time

def readFile(filename):
    with open(filename, 'r') as f:
        result = f.read()
    return result

def readDataFile(filename):
    return readFile(filename).strip().split('\n')

def sendQueries(queries):
    print('sending {} queries'.format(len(queries)))
    with makeDriver() as driver:
        with driver.session() as session:
            with session.begin_transaction() as tx:
                for query in queries:
                    tx.run(query)

def makeDriver():
    return gd.driver(uri, auth=('neo4j', password))

def setupFilenames(doMini=False):
    pre = ''
    if doMini:
        pre = 'mini_'
    filenames = ['{}m{}c.data'.format(pre, i) for i in ['v', 'o']]
    return filenames


def node(label, uid, name=''):
    return '({2}:{0} {{ uid: "{1}" }})'.format(label, uid, name)

def edge(label='EDGE', names=defaultNodeNames):
    return '({n0})-[:{label}]->({n1})'.format(n0=names[0], label=label, n1=names[1])

def mergeTwoNodesAndEdge(labels, uids):
    nodes = [node(labels[i], uids[i], v) for i, v in enumerate(defaultNodeNames)]
    return 'MERGE {} MERGE {} MERGE {};'.format(nodes[0], nodes[1], edge())


def deleteAll():
    sendQueries(['MATCH (n) DETACH DELETE n'])

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

def ftq(isFirst, doMini=True, send=False):
    print('ftq isFirst: {}  doMini: {}  send: {}'.format(isFirst, doMini, send))
    filenames = setupFilenames(doMini)
    pre = ''
    if not isFirst:
        pre = '2'
    data = globals()['readDataFiles' + pre](filenames)
    queries = globals()['makeQueries' + pre](data)
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
# method two produces queries only for unique lines in each data file
# method two is superior in terms of time (as can be seen via compareMethods)
# this is due to the time it takes to send queries, so the fewer the better

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
    return queries

def fileToQueries2(doMini=True, send=False):
    return ftq(False, doMini, send)

uri = readFile('bolt.txt')
password = readFile('password.txt')
defaultNodeNames = ['zero', 'one']
