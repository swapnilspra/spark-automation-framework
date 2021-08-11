       IDENTIFICATION DIVISION.
       PROGRAM-ID. NRS-WRK6520.
	     AUTHOR. DST. MODIFIED BY SHREENI.
	     DATE WRITTEN. 07/01/2009.
	     DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	         SELECT NRS-WRK6520 ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  NRS-WRK6520.
       01  NRS-WRK6520.                                                             
           05  RECORD-CODE                       PIC X(1).                      
           05  FUND-CODE                         PIC 9(7).                      
           05  DETAIL-DATA-AREA.                                                
               10  MGMT-CODE                     PIC X(2).                      
               10  STATE-CODE                    PIC 9(3).                      
               10  SUPER-SHEET-DATE              PIC 9(8).                      
               10  MTD-REPEAT-AMT-CDE            PIC X(1).                      
               10  MTD-REPEAT-AMT                PIC 9(15)V9(2).                
               10  SHARES-CODE                   PIC X(1).                      
               10  SHARE-DATA                    PIC 9(13)V9(4).                
               10  DOLLAR-CODE                   PIC X(1).                      
               10  AMOUNT-DATA                   PIC 9(15)V9(2).                
               10  MTD-REOPEN-AMT-CDE            PIC X(1).                      
               10  MTD-REOPEN-AMT                PIC 9(15)V9(2).                
           05  HEADER-DATA-AREA REDEFINES DETAIL-DATA-AREA.                     
               10  FILLER                        PIC X(5).                      
               10  SUPER-SHEET-DATE              PIC 9(8).                      
               10  MGMT-NAME                     PIC X(10).                     
               10  SYSTEM-CODE                   PIC X(3).                      
               10  SOURCE-CODE                   PIC X(1).                      
               10  FILE-TYPE                     PIC X(1).                      
               10  FILLER                        PIC X(57).                     
           05  TRAILER-DATA-AREA REDEFINES DETAIL-DATA-AREA.                    
               10  MTD-REPEAT-AMT-CDE            PIC X(1).                      
               10  MTD-REPEAT-AMT                PIC 9(15)V9(2).                
               10  TOTAL-RECS                    PIC 9(5).                      
               10  SHARES-CODE                   PIC X(1).                      
               10  SHARE-DATA                    PIC 9(13)V9(4).                
               10  DOLLAR-CODE                   PIC X(1).                      
               10  AMOUNT-DATA                   PIC 9(15)V9(2).                
               10  MTD-REOPEN-AMT-CDE            PIC X(1).                      
               10  MTD-REOPEN-AMT                PIC 9(15)V9(2).                
               10  FILLER                        PIC X(8).                      
           05  FILLER                            PIC X(27).   
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.