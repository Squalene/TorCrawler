from typing import *
import statistics as stat
import gzip
import os 
import json
import igraph as ig
from urllib.parse import urlparse # domain extraction
import cairocffi #for graph visualization

SAMPLE_FOLDER_IN = "data/samplePages"
DATA_FOLDER_IN = "data/pages"
DATA_FOLDER_OUT = "data/urls"

FILE_SUFFIX = ".gz"
URL_FILE = "all.json" + FILE_SUFFIX
GRAPH_FILE = "torNetwork.GraphML"


#Possible next step:

# Store the number of fetched pages of a certain domain as a node attribute
# Execute script on whole crawl=> must stop and restart the crawl (otherwise, it copies from changing files=> gzip parse error)
# Find more interesting characteristics to extract from graph

def main():

    extractURLAndMergeFiles(SAMPLE_FOLDER_IN,DATA_FOLDER_OUT,URL_FILE)

    #graph = buildGraphFromFile(DATA_FOLDER_OUT+"/"+URL_FILE)
    #graph = ig.Graph.Read_GraphML(DATA_FOLDER_OUT+"/"+GRAPH_FILE)
    #storeGraph(DATA_FOLDER_OUT+"/"+GRAPH_FILE,graph)
    #extractGraphFeatures(graph)




#Takes all the gzip files of a crawl, extract the urls and outlinks from it
#(remove the content and title of page) and merge them all in one json gzip file (in a list)
#The list can simply be reused by doing:
# with gzip.open(filePath,'rt', encoding='UTF-8') as zipFileIn:
#   list = json.load(zipFileIn)
#Args:
#folderIn: the folder where the crawl gzip pageContents are stored
#folderOut: the folder where the resulting merged file will be stored
#fileOut: the resulting out file

def extractURLAndMergeFiles(folderIn: str, folderOut: str, fileOut:str)->None:

    fileNames = [f for f in os.listdir(folderIn) if f.endswith(FILE_SUFFIX)]
    print(fileNames)
    fileOutPath = folderOut+"/"+fileOut

    with gzip.open(fileOutPath,"wt", encoding='UTF-8') as zipFileOut:
        zipFileOut.write("[\n")#Create a list of json objects
        for fileName in fileNames:
            with gzip.open(folderIn+"/"+fileName,'rt', encoding='UTF-8') as zipFileIn:
                #In order to catch error if file  was copied from an ongoing crawl
                try:
                
                    tempLine = zipFileIn.readline()#Used to be able to work on the last line as a particular case 
                    for line in zipFileIn:
                        jsonObject = json.loads(tempLine) #load string
                        jsonObject.pop("title",None) #delete if present
                        jsonObject.pop("content",None)
                        zipFileOut.write(json.dumps(jsonObject)+",\n")
                        tempLine = line

                    #Do the same as before
                    jsonObject = json.loads(tempLine) #load string
                    jsonObject.pop("title",None) #delete if present
                    jsonObject.pop("content",None)
                    zipFileOut.write(json.dumps(jsonObject))
                    
                    if(fileName!=fileNames[-1]):
                        zipFileOut.write(",\n")
                        
                except EOFError as e:
                    print(e)
                    print("Skipping  to next file")

        zipFileOut.write("\n]")

# Returns the directed weighted graph extracted from the file
# Uses igraph format
# Important: only .onion urls are kept, url to self are also kept

#Build a graph from only .onion websites and accepts self loop
def buildGraphFromFile(filePath: str)->ig.Graph:

    print("File path:"+filePath)
   
    urlList=None
    with gzip.open(filePath,'rt', encoding='UTF-8') as zipFileIn:
        urlList = json.load(zipFileIn) # loads the list in python 

    domainSet=set()

    #only keep .onion urls
    for page in urlList:
        if(".onion" in page["pageUrl"]):
            pageDomain = urlparse(page["pageUrl"]).netloc
            domainSet.add(pageDomain)

        for outlink in page["linkURLs"]:
            if(".onion" in outlink):
                outlinkDomain = urlparse(outlink).netloc
                domainSet.add(outlinkDomain)

    print(domainSet)
    #Map(domain,Map(domain,linkCount))
    graphMap = {}

    for domain in domainSet:
        graphMap[domain]={}

    print(graphMap)

    for page in urlList:
        if(".onion" in page["pageUrl"]):
            pageDomain = urlparse(page["pageUrl"]).netloc

            for outlink in page["linkURLs"]:
                if(".onion" in outlink):
                    outlinkDomain = urlparse(outlink).netloc
                    graphMap[pageDomain][outlinkDomain] = graphMap[pageDomain].get(outlinkDomain,0)+1

    tupleList = [] #List of (from,to,weights)

    for domain in graphMap:
        for outLinkedDomain in graphMap[domain]:
            tupleList.append((domain,outLinkedDomain,graphMap[domain][outLinkedDomain]))

    print(tupleList)
    #print(graphMap)

    graph =ig.Graph.TupleList(tupleList, weights=True, directed=True)

    graph["name"]="torNetwork"
    print(graph)

    return graph

def storeGraph(filePath:str, graph:ig.Graph)->None:

    ig.Graph.write_graphml(graph,filePath)



def extractGraphFeatures(graph:ig.Graph)->None:

    print(ig.Graph.summary(graph))
    print("Graph diameter is "+ str(ig.Graph.diameter(graph)))
    print("Average path length is "+ str(ig.Graph.average_path_length(graph)))
    print("Graph density is "+ str(ig.Graph.density(graph)))
    print("Average shortest path length is " + str(computeAverageShortestPaths(graph)))
    
    print("The top three domains in terms of pageRank are")
    print(*computePageRanks(graph)[:3],sep="\n")

    print("The top three domains in terms of in-degrees are")
    print(*computeDegrees(graph,"in")[:3],sep="\n")
    print("The top three domains in terms of out-degrees are")
    print(*computeDegrees(graph,"out")[:3],sep="\n")


    print("Average out-degree is " + str(stat.mean(graph.vs.degree(mode="out"))))#Save as out degree

    # layout = graph.layout_lgl()
    # ig.plot(graph,layout=layout,vertex_size=5,edge_arrow_width=0.2,sedge_arrow_size=1)

#Returns a decreasingly sorted list of domains according to their pageRanks List(domainName,pageRanks) 
def computePageRanks(graph:ig.Graph)->List[Tuple[str,int]]:
    pageRanks = ig.Graph.pagerank(graph)
    vertexRanks = list(zip(graph.vs["name"],pageRanks))
    vertexRanks.sort(key=lambda a: a[1],reverse=True)
    return vertexRanks

#Returns a decreasingly sorted list of domains according to their degree (can be in degree or out degree)
#mode: string: "in" for in-degrees and "out" for out-degrees
def computeDegrees(graph:ig.Graph,mode:str)->List[Tuple[str,int]]:
    vertexDegrees = list(zip(graph.vs["name"],graph.vs.degree(mode=mode)))
    vertexDegrees.sort(key=lambda a: a[1],reverse=True)
    return vertexDegrees



#Returns the length of the average shortest path
def computeAverageShortestPaths(graph:ig.Graph)->float:
    shortestPaths = ig.Graph.shortest_paths(graph)
    finitePaths = [item for elem in shortestPaths for item in elem if item!=ig.math.inf]#Convert list of list to list and remove infinite paths
    return stat.mean(finitePaths)




main()

#NOTE: not used
#From all the crawl gzip files in filePathIn, create gzip files into filePathOut only containing the url and the outlink urls
def extractURLFromDataAndWriteIt(filePathIn: str,filePathOut:str)->None:

    with gzip.open(filePathIn,'rt', encoding='UTF-8') as zipFileIn, gzip.open(filePathOut,"wt", encoding='UTF-8') as zipFileOut:
        for line in zipFileIn:
            jsonObject = json.loads(line) #load string
            jsonObject.pop("title",None) #delete if present
            jsonObject.pop("content",None)
            zipFileOut.write(json.dumps(jsonObject)+"\n")

#NOTE: Not used
#Merges all the gzip files in the folderInPath into one file (fileOutPath)
def mergeFiles(folderInPath: str,fileOutPath: str)->None:
    fileNames = [f for f in os.listdir(folderInPath) if f.endswith(FILE_SUFFIX)]
    print(fileNames)
    with gzip.open(fileOutPath,"wt", encoding='UTF-8') as zipFileOut:
        for fileName in fileNames:
            with gzip.open(folderInPath+"/"+fileName,'rt', encoding='UTF-8') as zipFileIn: 
                for line in zipFileIn:
                   zipFileOut.write(line)
