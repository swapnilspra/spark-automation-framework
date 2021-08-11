       IDENTIFICATION DIVISION.
       PROGRAM-ID. 857-REPORT.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 10/13/2010.
	   DATE COMPILED. 10/13/2010.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
        SELECT 857-REPORT ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  857-REPORT.
       01  857-REPORT.                                                             
           05  RECORD-TYPE-CD                    PIC X(1).                      
               88  RECORD-TYPE-HEADER                VALUE 'H'.                 
               88  RECORD-TYPE-DETAIL                VALUE 'D'.                 
               88  RECORD-TYPE-TRAILER               VALUE 'T'.                 
           05  HEADER-RECORD-AREA.                                              
               10  HEADER-DESCRIPTION-TX         PIC X(40).                     
               10  FILLER                        PIC X(2).                      
               10  TLR-PYO-PER-END-DT.                                          
                   15  TLR-PYO-PER-END-YYYY-DT   PIC 9(4).                      
                   15  TLR-PYO-PER-END-MM-DT     PIC 9(2).                      
                   15  TLR-PYO-PER-END-DD-DT     PIC 9(2).                      
               10  FILLER                        PIC X(2).                      
               10  TLR-PYO-WKO-NBR-ID            PIC X(8).                      
               10  FILLER                        PIC X(2).                      
               10  JOB-RUN-CD                    PIC X(4).                      
                   88  JOB-RUN-TEST                  VALUE 'TEST'.              
                   88  JOB-RUN-PROD                  VALUE 'PROD'.              
               10  FILLER                        PIC X(933).                    
           05  DETAIL-RECORD-AREA REDEFINES HEADER-RECORD-AREA.                 
               10  NSCC-INDICATOR-CD             PIC X(1).                      
                   88  NSCC-INDICATOR-YES            VALUE 'Y'.                 
                   88  NSCC-INDICATOR-NO             VALUE 'N'.                 
               10  NSCC-NAME-USE-CD              PIC X(1).                      
                   88  NSCC-NAME-USE-REPRESENTATIVE  VALUE 'R'.                 
                   88  NSCC-NAME-USE-CLIENT          VALUE 'C'.                 
               10  FD-SPNSR-SHRT-NME             PIC X(3).                      
               10  FUND-SPONSOR-NME              PIC X(35).                     
               10  FINANCIAL-INST-ID             PIC 9(7).                      
               10  FINCL-INST-NAME-1             PIC X(40).                     
               10  FI-PAY-OFC-ID                 PIC X(9).                      
               10  PAYMENT-METHOD-CD             PIC X(5).                      
                   88  PAYMENT-METHOD-CHECK          VALUE 'CHECK'.             
                   88  PAYMENT-METHOD-WIRE           VALUE 'WIRE '.             
                   88  PAYMENT-METHOD-ACH            VALUE 'ACH  '.             
               10  FINCL-INST-BRCH-ID            PIC X(9).                      
               10  FI-BRANCH-REP-ID              PIC X(9).                      
               10  FG-REP-NME                    PIC X(30).                     
               10  FUND-CODE                     PIC 9(7).                      
               10  MUTUAL-FUND-LNG-NM            PIC X(38).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  CMPN-PAYEE-TYPE-CD            PIC X(3).                      
               10  ACCOUNT-NUMBER                PIC 9(11).                     
               10  CHECK-DIGIT                   PIC 9(1).                      
               10  ALPHA-CODE                    PIC X(10).                     
               10  TLR-CMPN-FEE-CD               PIC X(4).                      
               10  SPLIT-COMPENSATION-CD         PIC X(1).                      
                   88  SPLIT-COMPENSATION-YES        VALUE 'Y'.                 
                   88  SPLIT-COMPENSATION-NO         VALUE 'N'.                 
               10  PRV-CMPN-SCH-ORR-TYP          PIC X(4).                      
               10  ELG-CMPN-SCH-ORR-TYP          PIC X(4).                      
               10  TLR-PAYOUT-TYP-CD             PIC X(1).                      
                   88  TLR-PAYOUT-TYP-AVERAGE-ASSETS VALUE 'A'.                 
                   88  TLR-PAYOUT-TYP-SLIDING-SCALE  VALUE 'S'.                 
                   88  TLR-PAYOUT-TYP-ASSET-RANGE    VALUE 'V'.                 
                   88  TLR-PAYOUT-TYP-BLENDED-ASSET  VALUE 'B'.                 
               10  PRE-AGREE-BASIS-POINT-RT      PIC -999.9.                    
               10  PRE-AGREE-ASSET-AT            PIC -9(15).9(2).               
               10  PRE-AGREE-COMPENSATION-AT     PIC -9(15).9(2).               
               10  PRV-SPLIT-COMPENSATION-AT     PIC -9(15).9(2).               
               10  ELIGIBLE-TABLE                                               
                                                 OCCURS     9.                  
                   15  LEVEL-CD                  PIC X(3).                      
                       88  LEVEL-L1                  VALUE ' L1'.               
                       88  LEVEL-L2                  VALUE ' L2'.               
                       88  LEVEL-L3                  VALUE ' L3'.               
                       88  LEVEL-L4                  VALUE ' L4'.               
                       88  LEVEL-L5                  VALUE ' L5'.               
                       88  LEVEL-L6                  VALUE ' L6'.               
                       88  LEVEL-L7                  VALUE ' L7'.               
                       88  LEVEL-L8                  VALUE ' L8'.               
                       88  LEVEL-L9                  VALUE ' L9'.               
                       88  LEVEL-FL                  VALUE ' FL'.               
                   15  ELIGIBLE-BASIS-POINT-RT   PIC -999.9.                    
                   15  ELIGIBLE-ASSET-AT         PIC -9(15).9(2).               
                   15  ELIGIBLE-COMPENSATION-AT  PIC -9(15).9(2).               
               10  TOTAL-ELIGIBLE-BASIS-POINT-RT PIC -999.9.                    
               10  TOTAL-ELIGIBLE-ASSET-AT       PIC -9(15).9(2).               
               10  TOTAL-ELIGIBLE-COMPENSATION-AT                               
                                                 PIC -9(15).9(2).               
               10  TOTAL-COMPENSATION-AT         PIC -9(15).9(2).               
               10  FILLER                        PIC X(208).                    
           05  TRAILER-RECORD-AREA REDEFINES HEADER-RECORD-AREA.                
               10  TRAILER-DESCRIPTION-TX        PIC X(25).                     
               10  TOTAL-RECORD-CT               PIC 9(9).                      
               10  TRAILER-RECORDS-TX            PIC X(8).                      
               10  FILLER                        PIC X(957).                    
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.                      
