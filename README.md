# invertedIndex y PageRank para búsqueda  

Descripción del repositorio en "cloud busqueda.pdf".  
  
Archivos:  

 /documentos/1.txt -> linea1: Título linea2: Abstract  
 /documentos/2.txt  
 /documentos/3.txt  
 /documentos/...  
 /documentos/100.txt  
  
 /links.txt -> linea1: 1 23 linea2: 1 12 linea3: 1 67 ... lineai: nodoFrom nodoTo -> grafo, del documento nodoFrom sale un link para el documento nodoTo  
   
 /outputInvertedIndex/caso/outputInvertedIndex.txt -> lineai: science 1,4,5,67 -> palabra y documentos separados por coma donde se encuentra esa palabra  
  
 /outputPageRank/caso/part-r-00000.txt -> lineai: 0.34 5 -> rank del documento 5 es 0.34, lineas ordenadas de rank menor a mayor para todos los documentos  
  
 -
