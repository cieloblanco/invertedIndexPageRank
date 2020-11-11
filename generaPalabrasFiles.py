from binary_file_search.BinaryFileSearch import BinaryFileSearch

def buscarRank(docID, pageRankFileName):
	with BinaryFileSearch(pageRankFileName, sep='\t', string_mode=False) as bfs:
		assert bfs.is_file_sorted()
		rank = bfs.search(docID)[0][1]
	return rank

def palabraFile(lineaDeInvIndex, pageRankFileName, palabraFileName):
	palabraFile = open(palabraFileName,'w')
	for i in lineaDeInvIndex[1:]:
		docID = i.strip('.tx')
		rank = buscarRank(int(docID), pageRankFileName)
		palabraFile.write(rank+' '+docID+'\n')
	palabraFile.close()
	return 0

def generaPalabrasFiles(invIndexFileName, pageRankFileName):
	invIndexFile = open(invIndexFileName, 'r')
	for l in invIndexFile:
		
		linea = l.split()
		palabraFileName = 'palabrasFiles/'+linea[0]+'.txt'
		palabraFile(linea, pageRankFileName, palabraFileName)
		
		#sort palabraFile		
		palabraFileSort = open(palabraFileName,'r')
		l = palabraFileSort.readlines()
		lineas = sorted(l, key=lambda x: float(x.split()[0]), reverse=True)
		palabraFileSort.close()
		palabraFileSort = open(palabraFileName,'w')
		for linea in lineas:
			palabraFileSort.write(linea)	
		palabraFileSort.close()
		
	invIndexFile.close()

generaPalabrasFiles('invertedIndex.txt', 'pageRank.txt') #checar que PR.txt no tenga la primera linea en blanco


