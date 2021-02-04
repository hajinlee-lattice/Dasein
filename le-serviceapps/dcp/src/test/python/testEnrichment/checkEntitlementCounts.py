import json, os, csv, getopt, sys
from dictor import dictor

recordTypes = { "Domain Use", "Domain Master Data Use", "Analytical Use" }

baseinfo = { "D-U-N-S Number","Primary Business Name","ISO Alpha 2 Char Country Code" }
comp_entity_res = { 
"Matched D-U-N-S Number",
"Match Type",
"Match Confidence Code",
"Match Grades",
"Match Data Profile",
"Name Match Score",
"Match Candidate Operating Status",
"Match Primary Business Name",
"Match ISO Alpha 2 Char Country Code"
}

def checkGroupElements( bidnameMap, result_elem ):

    doneblocks = []
    errorelement = []
    prev = ""
    for elem in result_elem:
        block = bidnameMap[elem]
        if ( block != prev ):
           if ( block in doneblocks ):
              print ( "ERROR: ", elem , " is not grouped with its block " , block)
              errorelement.append(elem)
           else:
              doneblocks.append( block)
              prev = block
    if ( len(errorelement) > 0 ):
       print("ERROR: Some elements are not grouped together within its block")
    else:
       print("SUCCESS: All elements are grouped together within its block")

# Get Data Block Elements of the Enrichment Layout given a source id
def getLayoutDBE(): 
   os.system("node get_layout.js")
   with open('response.json') as f:
     data = json.load(f)
     return ( data['elements'])

#Get the field names of the elements from the enriched file
def getResultElements(filename):
    fields = [] 

    with open(filename, 'r') as csvfile: 
        csvreader = csv.reader(csvfile) 
	
	# extracting field names through first row 
        fields = next(csvreader) 

    idx = fields.index("D-U-N-S Number")
    return(fields[idx:])

#Get the field names of the input elements from the result file
def getResultInputElements(filename):
    fields = [] 

# reading csv file 
    with open(filename, 'r') as csvfile: 
        csvreader = csv.reader(csvfile) 
	
	# extracting field names through first row 
        fields = next(csvreader) 

    eidx = fields.index("Matched D-U-N-S Number")
    return(fields[:eidx])

#Get the field names of the elements from the input file
def getInputElements(filename):

    fields = [] 

    with open(filename, 'r') as csvfile: 
        csvreader = csv.reader(csvfile) 
        fields = next(csvreader) 

    return(fields)

#Get the field names of the Company Entity Resolution elements from the result file
def getResultCERElements(filename):

    fields = [] 

# reading csv file 
    with open(filename, 'r') as csvfile: 
        csvreader = csv.reader(csvfile) 
        fields = next(csvreader) 

    sidx = fields.index("Matched D-U-N-S Number")
    eidx = fields.index("Match ISO Alpha 2 Char Country Code")
    return(fields[sidx:eidx+1])

#Compare the input elements with the result
def checkInputEements(inputfile, matchfile):
    # Get Enriched elements list from the result
    result_elem = getResultInputElements(matchfile)
    input_elem = getInputElements(inputfile)
    lnum = len(input_elem)

    if lnum != len(result_elem) :
       print("ERROR: Input Element counts in the source and the matched file don't match")
       print("       Source has %2d - Matched result file has %2d elements " %(lnum, len(result_elem)) )
    else:
       print("SUCCESS: Input Element counts in the source and the matched file match - Total elements - ", lnum )

    mismatchedNames = []
    for name in input_elem:
        if not name in result_elem:
           mismatchedNames.append( name )

    if ( len(mismatchedNames) > 0 ):
           print ("ERROR: Some Fields in Input list doesn't exist in the matched result file : "  )
           print (mismatchedNames)
    else:
           print ("SUCCESS: All the fields in Input list exist in the matched result file " )

#Compare the Entity Resolution elements with the result
def checkEntityResElements(inputfile, matchfile):
    # Get Enrichment Layout elements of the source 
    comp_entity_res = { 
        "Matched D-U-N-S Number",
        "Match Type",
        "Match Confidence Code",
        "Match Grade",
        "Match Data Profile",
        "Name Match Score",
        "Match Candidate Operating Status",
        "Match Primary Business Name",
        "Match ISO Alpha 2 Char Country Code"
    }
  
    # Get Enriched elements list from the result
    result_elem = getResultCERElements(matchfile)
    lnum = len(comp_entity_res)

    if lnum != len(result_elem) :
       print("ERROR: Entity Resolution Element counts in the source and the matched file don't match")
       print("       Source has %2d - Matched result file has %2d elements " %(lnum, len(result_elem)) )
    else:
       print("SUCCESS: Entity Resolution Element counts in the source and the matched file match - Total elements - ", lnum )

    mismatchedNames = []
    for name in comp_entity_res:
        if not name in result_elem:
           mismatchedNames.append( name )

    if ( len(mismatchedNames) > 0 ):
           print ("ERROR: Some Fields in Company Entity Resolution list doesn't exist in the matched result file : "  )
           print (mismatchedNames)
    else:
           print ("SUCCESS: All the fields in Company Entity Resolution list exist in the matched result file " )

#Compare the enriched result file with the source enrichment layout and check if all the elements are present

#Compare the Enriched Data Block elements in the result with the enrichment layout in source
def checkEnrichedElements(inputfile, matchfile):
    # Get Enrichment Layout elements of the source 
    layout_elements = getLayoutDBE()
  
    # Get map of elementId and Display name
    bmap, emap = getElementMap()
    nameMap = getDisplayNames( layout_elements, emap )

    # Get Enriched elements list from the result
    result_elem = getResultElements(matchfile)
    lnum = len(layout_elements)

    if lnum != len(result_elem) :
       print("ERROR: Data Block Element counts in the source and the matched file don't match")
       print("       Source has %2d - Matched result file has %2d elements " %(lnum, len(result_elem)) )
    else:
       print("SUCCESS: Data Block Element counts in the source and the matched file match - Total elements - ", lnum )

    mismatchedNames = []
    for name in nameMap:
        if not nameMap[name] in result_elem:
           mismatchedNames.append( nameMap[name] )

    if ( len(mismatchedNames) > 0 ):
           print ("ERROR: Some Fields in Enrichment layout doesn't exist in the matched result file : "  )
           print (mismatchedNames)
    else:
           print ("SUCCESS: All the fields in Enrichment layout exist in the matched result file " )

    checkGroupElements( bmap, result_elem )

#Get display names of the elements
def getDisplayNames(layoutIds, emap):
    nameMap = {}
    for elemId in layoutIds:
        nameMap[elemId] = emap[elemId]
    return nameMap;

#Get a map of element id and Display names
def getElementMap():
   os.system("node get_tenant_entitlement.js")
   with open('response.json') as f:
      data = json.load(f)
   res, out = flatten_json(data)
   return(res, out)

# Helper method to process the json file
def flatten_json(nested_json):
    emap = {}
    out = {}
    blockid = None
    bid_dnameMap = {}

    def flatten(x, name='', blockid = None ):
        if type(x) is dict:
            for a in x:
                if ( a == "blockId" ):
                   blockid = x[a]
                if ( a == "elementId" ):
                   elementId = x[a]
                if ( a == "displayName" ):
                   emap[elementId] = x[a]
                   bid_dnameMap[x[a]] = blockid
              
                flatten(x[a], name + a + '_', blockid )
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_', blockid  )
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return (bid_dnameMap, emap)


def main(argv):
   if (len(argv)!=4):
      print('checkEntitlementCounts.py -i <inputfile> -m <matchfile>')
      sys.exit()
   inputfile = ''
   matchfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:m:",["ifile=","mfile="])
   except getopt.GetoptError:
      print('checkEntitlementCounts.py -i <inputfile> -m <matchfile>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('checkEntitlementCounts.py -i <inputfile> -m <matchfile>')
         sys.exit()
      elif (len(argv)!=4):
         print('checkEntitlementCounts.py -i <inputfile> -m <matchfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-m", "--mfile"):
         matchfile = arg

   checkEnrichedElements(inputfile, matchfile)
   checkEntityResElements(inputfile, matchfile)
   checkInputEements(inputfile, matchfile)


if __name__ == "__main__":
    main(sys.argv[1:])
