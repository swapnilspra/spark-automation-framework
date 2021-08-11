       IDENTIFICATION DIVISION.
       PROGRAM-ID. FDP-DEALER-POSITION.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT FDP-DEALER-POSITION ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  FDP-DEALER-POSITION.
	   01  FDP-DEALER-POSITION.                                                             
           05  RECORD-TYPE-CD                    PIC X(3).                      
               88  RECORD-TYPE-HEADER                VALUE 'FDH'.               
               88  RECORD-TYPE-TRAILER               VALUE 'FDT'.               
               88  RECORD-TYPE-DEALER-POSITION       VALUE 'FDP'.               
           05  SEQUENCE-NUMBER-ID                PIC 9(3).                      
           05  DETAIL-RECORD-1-AREA.                                            
               10  FI-FUND-GROUP-NBR             PIC 9(7).                      
               10  FINANCIAL-INST-ID             PIC 9(7).                      
               10  LAST-MNT-DATE-TIME            PIC X(26).                     
               10  LAST-MNT-OPID                 PIC X(8).                      
               10  FI-ESTABLISHED-DTE            PIC X(10).                     
               10  FI-CLOSED-DTE                 PIC X(10).                     
               10  FINCL-INST-NAME-1             PIC X(35).                     
               10  FI-TAX-ID-NBR                 PIC 9(9).                      
               10  FI-SLS-AGMT-REQD              PIC X(1).                      
                   88  FI-SLS-AGMT-REQD-YES          VALUE 'Y'.                 
                   88  FI-SLS-AGMT-REQD-NO           VALUE 'N'.                 
                   88  FI-SLS-AGMT-REQD-NA           VALUE 'Z'.                 
               10  FI-12B1-AGMT-REQD             PIC X(1).                      
                   88  FI-12B1-AGMT-REQD-YES         VALUE 'Y'.                 
                   88  FI-12B1-AGMT-REQD-NO          VALUE 'N'.                 
                   88  FI-12B1-AGMT-REQD-NA          VALUE 'Z'.                 
               10  FI-12B1-PAY-FREQ              PIC X(1).                      
                   88  FI-12B1-PAY-FREQ-A            VALUE 'A'.                 
                   88  FI-12B1-PAY-FREQ-M            VALUE 'M'.                 
                   88  FI-12B1-PAY-FREQ-Q            VALUE 'Q'.                 
                   88  FI-12B1-PAY-FREQ-S            VALUE 'S'.                 
                   88  FI-12B1-PAY-FREQ-NA           VALUE 'Z'.                 
               10  FI-12B1-TAPE-CDE              PIC X(1).                      
                   88  FI-12B1-TAPE-YES              VALUE 'Y'.                 
                   88  FI-12B1-TAPE-NO               VALUE 'N'.                 
                   88  FI-12B1-TAPE-NA               VALUE 'Z'.                 
               10  FI-AUTO-DIV-CHK-CD            PIC X(1).                      
                   88  FI-AUTO-DIV-CHK-Y             VALUE 'Y'.                 
                   88  FI-AUTO-DIV-CHK-N             VALUE 'N'.                 
                   88  FI-AUTO-DIV-CHK-NA            VALUE 'Z'.                 
               10  WIRE-INCL-ACRL-CDE            PIC X(1).                      
                   88  DLR-WIRE-INCLUDE-ACCRUAL-YES  VALUE 'Y'.                 
                   88  DLR-WIRE-INCLUDE-ACCRUAL-NO   VALUE 'N'.                 
               10  SIPC-INSURED-CDE              PIC X(1).                      
                   88  SIPC-INSURED-YES              VALUE 'Y'.                 
                   88  SIPC-INSURED-NO               VALUE 'N'.                 
                   88  SIPC-INSURED-NOT-AVAIL        VALUE 'Z'.                 
               10  SELLS-IN-FDIC-CDE             PIC X(1).                      
                   88  SELLS-IN-FDIC-INST-YES        VALUE 'Y'.                 
                   88  SELLS-IN-FDIC-INST-NO         VALUE 'N'.                 
                   88  SELLS-IN-FDIC-INST-NOT-AVAIL  VALUE 'Z'.                 
               10  REPORT-SORT-SEQ-CD            PIC X(1).                      
                   88  REPORT-SORT-SEQ-FUND          VALUE 'F'.                 
                   88  REPORT-SORT-SEQ-DEF-CONTRB-PLN                           
                                                     VALUE 'P'.                 
               10  RPT-PRN-MDA-LVL-CD            PIC X(1).                      
                   88  RPT-PRN-MDA-LVL-DETAIL        VALUE 'D'.                 
                   88  RPT-PRN-MDA-LVL-DEF-CONTRB-PLN                           
                                                     VALUE 'P'.                 
               10  RPT-EDI-MDA-LVL-CD            PIC X(1).                      
                   88  RPT-EDI-MDA-LVL-DETAIL        VALUE 'D'.                 
                   88  RPT-EDI-MDA-LVL-DEF-CONTRB-PLN                           
                                                     VALUE 'P'.                 
               10  FINS-ID                       PIC 9(7).                      
               10  TIPS-REFRESHER-ID             PIC X(4).                      
               10  FILLER                        PIC X(20).                     
           05  DETAIL-RECORD-2-AREA REDEFINES DETAIL-RECORD-1-AREA.             
               10  FINCL-INST-NAME-2             PIC X(40).                     
               10  FI-MEMBER-TYPE-CDE            PIC X(2).                      
                   88  FI-MEMBER-TYPE-BROKER         VALUE '00'.                
                   88  FI-MEMBER-TYPE-TRUSTEE        VALUE '01'.                
                   88  FI-MEMBER-TYPE-THIRD-PRTY-ADMN                           
                                                     VALUE '02'.                
                   88  FI-MEMBER-TYPE-NON-NSCC-TRD   VALUE 'NN'.                
               10  TRUSTEE-DFL-DLR-ID            PIC 9(7).                      
               10  FI-FG-SLS-CHN-TYPE            PIC X(3).                      
               10  MERGED-TO-FG-FI-ID            PIC 9(7).                      
               10  FI-STM-FRQ-CD                 PIC X(2).                      
                   88  FI-STM-FRQ-ANNUALLY           VALUE 'AN'.                
                   88  FI-STM-FRQ-QUARTERLY          VALUE 'QT'.                
                   88  FI-STM-FRQ-MONTHLY-ONLY       VALUE 'MN'.                
                   88  FI-STM-FRQ-MONTHLY-QUARTERLY  VALUE 'MQ'.                
                   88  FI-STM-FRQ-NA                 VALUE 'ZZ'.                
               10  DIVIDEND-CONFIRM-COPY-CT      PIC 9(3).                      
               10  DIV-CONFIRM-SUPPRESSION-CD    PIC X(1).                      
                   88  DIV-CONFIRM-SUPPRESSION-YES   VALUE 'Y'.                 
                   88  DIV-CONFIRM-SUPPRESSION-NO    VALUE 'N'.                 
               10  DAILY-CONFIRM-COPY-CT         PIC 9(3).                      
               10  DAILY-CONFIRM-SUPPRESSION-CD  PIC X(1).                      
                   88  DAILY-CONFIRM-SUPPRESSION-YES VALUE 'Y'.                 
                   88  DAILY-CONFIRM-SUPPRESSION-NO  VALUE 'N'.                 
               10  DEALER-FAIL-TRADE-FULL-COPY-CT                               
                                                 PIC 9(3).                      
               10  DEALER-FAIL-TRADE-500K-COPY-CT                               
                                                 PIC 9(3).                      
               10  DEALER-NEW-TRADE-FULL-COPY-CT PIC 9(3).                      
               10  DEALER-NEW-TRADE-500K-COPY-CT PIC 9(3).                      
               10  DLR-PROCESS-TRADE-FULL-COPY-CT                               
                                                 PIC 9(3).                      
               10  DLR-PROCESS-TRADE-500K-COPY-CT                               
                                                 PIC 9(3).                      
               10  DEALER-CML-DLQ-TRADE-COPY-CT  PIC 9(3).                      
               10  DLR-WIRE-ORDER-CONFIRM-COPY-CT                               
                                                 PIC 9(3).                      
               10  DLR-PERIODIC-STATEMENT-COPY-CT                               
                                                 PIC 9(3).                      
               10  FIN-IMY-CLASSIFICATION-ABR-NM PIC X(4).                      
               10  UNRL-FIRM-SYMBOL-ID           PIC X(4).                      
               10  FILLER                        PIC X(50).                     
           05  DETAIL-RECORD-3-AREA REDEFINES DETAIL-RECORD-1-AREA.             
               10  FI-REP-NBR-FRMT               PIC X(9).                      
               10  FI-REP-NME-FORMAT             PIC X(30).                     
               10  ONLIN-INVLD-REP-CD            PIC X(1).                      
                   88  ONLIN-INVLD-REP-DEF           VALUE 'D'.                 
                   88  ONLIN-INVLD-REP-REJ           VALUE 'R'.                 
               10  FI-STATE-LIC-VLD              PIC X(1).                      
                   88  FI-STATE-LIC-VLD-YES          VALUE 'Y'.                 
                   88  FI-STATE-LIC-VLD-NO           VALUE 'N'.                 
               10  FI-INVLD-ST-LIC-CD            PIC X(1).                      
                   88  FI-INVLD-ST-LIC-A             VALUE 'A'.                 
                   88  FI-INVLD-ST-LIC-R             VALUE 'R'.                 
                   88  FI-INVLD-ST-LIC-NA            VALUE 'Z'.                 
               10  FI-NASD-LIC-VLD               PIC X(1).                      
                   88  FI-NASD-LIC-VLD-YES           VALUE 'Y'.                 
                   88  FI-NASD-LIC-VLD-NO            VALUE 'N'.                 
               10  FI-INVLD-NASD-LIC             PIC X(1).                      
                   88  FI-INVLD-NASD-LIC-A           VALUE 'A'.                 
                   88  FI-INVLD-NASD-LIC-R           VALUE 'R'.                 
                   88  FI-INVLD-NASD-LIC-NA          VALUE 'Z'.                 
               10  BATCH-INVLD-REP-CD            PIC X(1).                      
                   88  BATCH-INVLD-REP-REJ           VALUE 'R'.                 
                   88  BATCH-INVLD-REP-DEF           VALUE 'D'.                 
                   88  BATCH-INVLD-REP-BYP           VALUE 'B'.                 
                   88  BATCH-INVLD-REP-EST           VALUE 'E'.                 
                   88  BATCH-INVLD-REP-ESB           VALUE 'F'.                 
                   88  BATCH-INVLD-REP-ESD           VALUE 'G'.                 
               10  FI-REP-LEVEL-CDE              PIC X(1).                      
                   88  FI-REP-LEVEL-YES              VALUE 'Y'.                 
                   88  FI-REP-LEVEL-NO               VALUE 'N'.                 
               10  FI-REP-CONVRSN-CDE            PIC X(1).                      
                   88  FI-REP-CONVRSN-YES            VALUE 'Y'.                 
                   88  FI-REP-CONVRSN-NO             VALUE 'N'.                 
               10  REP-NBR-ALIAS-CDE             PIC X(1).                      
                   88  REP-NBR-ALIAS-YES             VALUE 'Y'.                 
                   88  REP-NBR-ALIAS-NO              VALUE 'N'.                 
               10  REP-NAME-ALIAS-CDE            PIC X(1).                      
                   88  REP-NAME-ALIAS-YES            VALUE 'Y'.                 
                   88  REP-NAME-ALIAS-NO             VALUE 'N'.                 
               10  REP-NBR-USED-CDE              PIC X(1).                      
                   88  REP-NBR-USED-YES              VALUE 'Y'.                 
                   88  REP-NBR-USED-NO               VALUE 'N'.                 
               10  REP-NM-REQUIRED-CD            PIC X(1).                      
                   88  REP-NM-REQUIRED-YES           VALUE 'Y'.                 
                   88  REP-NM-REQUIRED-NO            VALUE 'N'.                 
               10  RP-ERR-LOOKP-VR-CD            PIC X(1).                      
                   88  RP-ERR-LOOKP-VR-YES           VALUE 'Y'.                 
                   88  RP-ERR-LOOKP-VR-NO            VALUE 'N'.                 
               10  ADD-REP-OPTION-CD             PIC X(1).                      
                   88  ADD-REP-OPTION-YES            VALUE 'Y'.                 
                   88  ADD-REP-OPTION-NO             VALUE 'N'.                 
               10  REP-EDIT-OVERID-CD            PIC X(1).                      
                   88  REP-EDIT-OVERID-YES           VALUE 'Y'.                 
                   88  REP-EDIT-OVERID-NO            VALUE 'N'.                 
               10  IVT-SLIP-ADR-CD               PIC X(1).                      
                   88  IVT-SLIP-ADR-FI               VALUE 'F'.                 
                   88  IVT-SLIP-ADR-BRANCH           VALUE 'B'.                 
                   88  IVT-SLIP-ADR-MUTUAL-FUND      VALUE 'M'.                 
                   88  IVT-SLIP-ADR-NA               VALUE 'Z'.                 
               10  FG-FI-EXTERNAL-ID             PIC X(20).                     
               10  FI-FG-DLD-FILE-FORMAT-ABR-NM  PIC X(4).                      
               10  FI-FG-APPROVAL-RATING-ABR-NM  PIC X(4).                      
               10  FI-TRUSTEE-TAX-RPT-RSP-CD     PIC X(1).                      
                   88  FI-TRUSTEE-TAX-RPT-RSP-YES    VALUE 'Y'.                 
                   88  FI-TRUSTEE-TAX-RPT-RSP-NO     VALUE 'N'.                 
               10  FILLER                        PIC X(70).                     
           05  DETAIL-HEADER-RECORD-AREA REDEFINES                              
               DETAIL-RECORD-1-AREA.                                            
               10  HEADER-FILE-TX                PIC X(15).                     
               10  SUPER-SHEET-DT.                                              
                   15  SUPER-SHEET-YYYY-DT       PIC 9(4).                      
                   15  SUPER-SHEET-MM-DT         PIC 9(2).                      
                   15  SUPER-SHEET-DD-DT         PIC 9(2).                      
               10  PROCESSED-DT.                                                
                   15  PROCESSED-YYYY-DT         PIC 9(4).                      
                   15  PROCESSED-MM-DT           PIC 9(2).                      
                   15  PROCESSED-DD-DT           PIC 9(2).                      
               10  PROCESSED-TM.                                                
                   15  PROCESSED-HH-TM           PIC 9(2).                      
                   15  PROCESSED-MM-TM           PIC 9(2).                      
                   15  PROCESSED-SS-TM           PIC 9(2).                      
               10  JOB-NM                        PIC X(8).                      
               10  REF-FORMAT-CDE                PIC 9(3).                      
                   88  REF-FORMAT-TIP                VALUE 000.                 
                   88  REF-FORMAT-NSCC               VALUE 001.                 
                   88  REF-FORMAT-BOSS               VALUE 002.                 
                   88  REF-FORMAT-TIP-DIRECT         VALUE 003.                 
                   88  REF-FORMAT-TIP-ORDER          VALUE 004.                 
                   88  REF-FORMAT-TIP-GLOBAL         VALUE 099.                 
               10  REF-RQST-NBR                  PIC 9(7).                      
               10  FILLER                        PIC X(99).                     
           05  DETAIL-TRAILER-RECORD-AREA REDEFINES                             
               DETAIL-RECORD-1-AREA.                                            
               10  TRAILER-FILE-TX               PIC X(15).                     
               10  TOTAL-RECORD-CT               PIC 9(9).                      
               10  FILLER                        PIC X(130).    
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.			   
                  
