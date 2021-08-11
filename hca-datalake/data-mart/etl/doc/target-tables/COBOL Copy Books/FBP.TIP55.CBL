       IDENTIFICATION DIVISION.
       PROGRAM-ID. FBP-BRANCH-POSITION.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT FBP-BRANCH-POSITION ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  FBP-BRANCH-POSITION.
       01  FBP-BRANCH-POSITION.                                                             
           05  RECORD-TYPE-CD                    PIC X(3).                      
               88  RECORD-TYPE-BRANCH-POSITION       VALUE 'FBP'.               
           05  SEQUENCE-NUMBER-ID                PIC 9(3).                      
           05  DETAIL-RECORD-1-AREA.                                            
               10  FI-FUND-GROUP-NBR             PIC 9(7).                      
               10  FINANCIAL-INST-ID             PIC 9(7).                      
               10  FINCL-INST-BRCH-ID            PIC X(9).                      
               10  LAST-MNT-DATE-TIME            PIC X(26).                     
               10  LAST-MNT-OPID                 PIC X(8).                      
               10  FI-BRCH-ESTB-DTE              PIC X(10).                     
               10  FI-BRCH-CLOSED-DTE            PIC X(10).                     
               10  FI-BRCH-NAME                  PIC X(35).                     
               10  FI-BRCH-PAY-OFC-ID            PIC X(9).                      
               10  FI-BR-DFLT-REP-ID             PIC X(9).                      
               10  FI-REP-NBR-FRMT               PIC X(9).                      
               10  FI-FG-BR-FN-TYP-ID            PIC X(3).                      
               10  FILLER                        PIC X(12).                     
           05  DETAIL-RECORD-2-AREA REDEFINES DETAIL-RECORD-1-AREA.             
               10  FI-REP-NME-FORMAT             PIC X(30).                     
               10  BR-TRD-RESTRICT-CD            PIC X(1).                      
                   88  BR-TRD-RESTRICT-YES           VALUE 'Y'.                 
                   88  BR-TRD-RESTRICT-NO            VALUE 'N'.                 
               10  BR-ORR-BCH-INV-REP            PIC X(1).                      
                   88  BR-ORR-BCH-INV-REP-REJ        VALUE 'R'.                 
                   88  BR-ORR-BCH-INV-REP-DEF        VALUE 'D'.                 
                   88  BR-ORR-BCH-INV-REP-BYP        VALUE 'B'.                 
                   88  BR-ORR-BCH-INV-REP-EST        VALUE 'E'.                 
                   88  BR-ORR-BCH-INV-REP-ESB        VALUE 'F'.                 
                   88  BR-ORR-BCH-INV-REP-ESD        VALUE 'G'.                 
                   88  BR-ORR-BCH-INV-REP-NA         VALUE 'Z'.                 
               10  EXE-FIN-INST-ID               PIC 9(7).                      
               10  EXE-FIN-BRCH-ID               PIC X(9).                      
               10  FI-BRCH-ADDR-1-TXT            PIC X(35).                     
               10  FI-BRCH-ADDR-2-TXT            PIC X(35).                     
               10  FI-BRCH-ADDR-3-TXT            PIC X(35).                     
               10  FILLER                        PIC X(1).                      
           05  DETAIL-RECORD-3-AREA REDEFINES DETAIL-RECORD-1-AREA.             
               10  FI-BRCH-ADDR-4-TXT            PIC X(35).                     
               10  ORR-FI-BR-ML-NM-1             PIC X(35).                     
               10  ORR-FI-BR-ML-NM-2             PIC X(35).                     
               10  FI-BRCH-CITY-NME              PIC X(35).                     
               10  FILLER                        PIC X(14).                     
           05  DETAIL-RECORD-4-AREA REDEFINES DETAIL-RECORD-1-AREA.             
               10  FI-BR-CITY-CDE                PIC X(3).                      
               10  FI-BRCH-ST-CO-CDE             PIC X(3).                      
               10  FI-BRCH-ZIP-CDE               PIC X(9).                      
               10  FI-BR-TERRITORY-CD            PIC X(3).                      
               10  FI-BR-FOREIGN-TAX             PIC 9(5)V9(10).                
               10  FI-BRCH-FORGN-ZIP             PIC X(11).                     
               10  BR-STM-FRQ-CD                 PIC X(2).                      
                   88  BR-STM-FRQ-ANNUALLY           VALUE 'AN'.                
                   88  BR-STM-FRQ-QUARTERLY          VALUE 'QT'.                
                   88  BR-STM-FRQ-MONTHLY-ONL        VALUE 'MN'.                
                   88  BR-STM-FRQ-MONTHLY-QUARTERLY  VALUE 'MQ'.                
                   88  BR-STM-FRQ-NA                 VALUE 'ZZ'.                
               10  BR-MERGED-TO-FI-ID            PIC 9(7).                      
               10  BR-MERGED-TO-BR-ID            PIC X(9).                      
               10  FILLER                        PIC X(92).                     
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.				   
