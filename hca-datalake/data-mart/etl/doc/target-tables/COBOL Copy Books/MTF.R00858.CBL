       IDENTIFICATION DIVISION.
       PROGRAM-ID. 858-REPORT.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 10/13/2010.
	   DATE COMPILED. 10/13/2010.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
        SELECT 858-REPORT ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  858-REPORT.
       01  858-REPORT.                                                             
           05  RECORD-TYPE-CD                    PIC X(1).                      
               88  RECORD-TYPE-HEADER                VALUE 'H'.                 
               88  RECORD-TYPE-DETAIL                VALUE 'D'.                 
           05  HEADER-RECORD-AREA.                                              
               10  TLR-PYO-PER-BEG-DT.                                          
                   15  TLR-PYO-PER-BEG-YYYY      PIC 9(4).                      
                   15  TLR-PYO-PER-BEG-MM        PIC 9(2).                      
                   15  TLR-PYO-PER-BEG-DD        PIC 9(2).                      
               10  TLR-PYO-PER-END-DT.                                          
                   15  TLR-PYO-PER-END-YYYY      PIC 9(4).                      
                   15  TLR-PYO-PER-END-MM        PIC 9(2).                      
                   15  TLR-PYO-PER-END-DD        PIC 9(2).                      
               10  TLR-PYO-WKO-NBR-ID            PIC X(8).                      
               10  TLR-PYO-TST-RUN-CD            PIC X(1).                      
                   88  TLR-PYO-TST-RUN-YES           VALUE 'Y'.                 
                   88  TLR-PYO-TST-RUN-NO            VALUE 'N'.                 
               10  TLR-FEE-PYO-GRP-ID            PIC 9(5).                      
               10  TLR-FEE-PYO-GRP-NM            PIC X(35).                     
               10  FILLER                        PIC X(144).                    
           05  DETAIL-RECORD-AREA REDEFINES HEADER-RECORD-AREA.                 
               10  CMPN-PAYEE-TYPE-CD            PIC X(3).                      
               10  FINANCIAL-INST-ID             PIC 9(7).                      
               10  FINCL-INST-NAME-1             PIC X(40).                     
               10  FUND-CODE                     PIC 9(7).                      
               10  MUTUAL-FUND-SHT-NM            PIC X(10).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  TLR-CMPN-FEE-CD               PIC X(4).                      
               10  PREAGREE-CMPN-SCH-ORR-TYP     PIC X(5).                      
               10  PREAGREE-SHARE-RT             PIC 9(5)V9(10).                
               10  PREAGREE-AVERAGE-ASSETS-AT    PIC 9(15)V9(2).                
               10  PREAGREE-COMPENSATION-AT      PIC 9(15)V9(2).                
               10  TLR-PAYOUT-TYP-CD             PIC X(1).                      
                   88  TLR-PAYOUT-TYPE-AVERAGE-ASSETS                           
                                                     VALUE 'A'.                 
                   88  TLR-PAYOUT-TYP-SLIDING-SCALE  VALUE 'S'.                 
                   88  TLR-PAYOUT-TYP-ASSET-RANGE    VALUE 'V'.                 
                   88  TLR-PAYOUT-TYP-BLENDED-ASSET  VALUE 'B'.                 
               10  ELIGIBLE-CMPN-SCH-ORR-TYP     PIC X(5).                      
               10  ELIGIBLE-SHARE-RT             PIC 9(5)V9(10).                
               10  ELIGIBLE-AVERAGE-ASSETS-AT    PIC 9(15)V9(2).                
               10  ELIGIBLE-COMPENSATION-AT      PIC 9(15)V9(2).                
               10  TOTAL-COMPENSATION-AT         PIC 9(13)V9(2).                
               10  FILLER                        PIC X(5).                     
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.                      
