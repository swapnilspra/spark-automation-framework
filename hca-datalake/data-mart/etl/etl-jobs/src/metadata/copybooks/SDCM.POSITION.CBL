       IDENTIFICATION DIVISION.
       PROGRAM-ID. SDCM-CASH-MOVEMENT.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT SDCM-CASH-MOVEMENT ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  SDCM-CASH-MOVEMENT.
       01  SDCM-CASH-MOVEMENT.                                                             
           05  RECORD-TYPE-CD                    PIC X(1).                      
               88  RECORD-TYPE-HEADER                VALUE 'H'.                 
               88  RECORD-TYPE-DETAIL                VALUE 'D'.                 
               88  RECORD-TYPE-TRAILER               VALUE 'T'.                 
           05  HEADER-RECORD-AREA.                                              
               10  SYSTEM-ID                     PIC X(3).                      
               10  RUN-DTE-TME                   PIC X(26).                     
               10  RUN-1-ID                      PIC 9(11).                     
               10  RUN-2-ID                      PIC 9(11).                     
               10  MTPFG-TRD-STA-CDE             PIC X(1).                      
                   88  MTPFG-TRD-STA-OPEN            VALUE 'O'.                 
                   88  MTPFG-TRD-STA-CLOSED          VALUE 'C'.                 
                   88  MTPFG-TRD-STA-RED-FINAL       VALUE 'F'.                 
                   88  MTPFG-TRD-STA-REV-CLOSED      VALUE 'R'.                 
                   88  MTPFG-TRD-STA-NIGHTLY-OPEN    VALUE 'N'.                 
               10  FILLER                        PIC X(417).                    
           05  DETAIL-RECORD-AREA REDEFINES HEADER-RECORD-AREA.                 
               10  RECORD-CD                     PIC 9(3).                      
                   88  RECORD-VOLUNTARY-TRANSACTION  VALUE 001.                 
                   88  RECORD-CONTRACTUAL-TRANSACTION                           
                                                     VALUE 002.                 
                   88  RECORD-ORDER-PROCESSING-TR    VALUE 005.                 
               10  MGMT-CODE                     PIC X(2).                      
               10  FUND-CODE                     PIC 9(7).                      
               10  TRANSFER-SHEET-GROUP          PIC 9(3).                      
               10  SS-DATE.                                                     
                   15  SS-YYYY                   PIC 9(4).                      
                   15  SS-MM                     PIC 9(2).                      
                   15  SS-DD                     PIC 9(2).                      
               10  LOAD-ACCOUNT-NUMBER           PIC 9(11).                     
               10  NOLOAD-ACCOUNT-NUMBER         PIC 9(11).                     
               10  ALPHA-CODE                    PIC X(10).                     
               10  ORDER-NUMBER                  PIC 9(9).                      
               10  TRANSACTION-CODE              PIC 9(3).                      
               10  TRANSACTION-SUFFIX            PIC 9(3).                      
               10  BATCH-NUMBER                  PIC 9(7).                      
               10  REASON-CODE                   PIC 9(3).                      
               10  FINANCIAL-INST-ID             PIC 9(7).                      
               10  FINCL-INST-BRCH-ID            PIC X(9).                      
               10  CUMULATIVE-DISC-NO            PIC 9(9).                      
               10  LOI-NUMBER                    PIC 9(9).                      
               10  SOCIAL-CODE                   PIC 9(3).                      
               10  ISSUE-CODE                    PIC 9(1).                      
               10  TRADE-DATE.                                                  
                   15  TRADE-DATE-YYYY           PIC 9(4).                      
                   15  TRADE-MONTH               PIC 9(2).                      
                   15  TRADE-DAY                 PIC 9(2).                      
               10  SHARES                        PIC S9(11)V9(4)                
                    SIGN TRAILING SEPARATE.                                     
               10  OFFERING-PRICE                PIC 9(5)V9(4).                 
               10  GROSS-AMOUNT                  PIC S9(13)V9(2)                
                    SIGN TRAILING SEPARATE.                                     
               10  DEALER-COMMISSION             PIC S9(13)V9(2)                
                    SIGN TRAILING SEPARATE.                                     
               10  UNDERWRITER-COMMISSION        PIC S9(13)V9(2)                
                    SIGN TRAILING SEPARATE.                                     
               10  NET-ASSET-VALUE-AMT           PIC S9(13)V9(2)                
                    SIGN TRAILING SEPARATE.                                     
               10  DISC-CATEGORY-CDE             PIC 9(2).                      
               10  DEALER-COMMISSION-CODE        PIC X(1).                      
                   88  GROSS-TRADE                   VALUE 'G'.                 
                   88  NET-TRADE                     VALUE 'N'.                 
               10  TRANSACTION-TYPE-CD           PIC X(1).                      
                   88  TRANSACTION-TYPE-PURCHASE     VALUE '0'.                 
                   88  TRANSACTION-TYPE-REDEMPTION   VALUE '1'.                 
               10  LOAD-NOLOAD-CODE              PIC 9(1).                      
                   88  LOAD                          VALUE 0.                   
                   88  NOLOAD                        VALUE 1.                   
               10  NEW-ACCOUNT-CD                PIC 9(1).                      
                   88  NEW-ACCOUNT-YES               VALUE 0.                   
                   88  NEW-ACCOUNT-NO                VALUE 1.                   
               10  CERTIFICATE-CD                PIC X(1).                      
                   88  CERTIFICATE-DEPOSIT           VALUE 'D'.                 
                   88  CERTIFICATE-WITHDRAWAL        VALUE 'W'.                 
               10  RESIDENT-STATE-COUNTRY        PIC 9(3).                      
               10  CERTIFICATE-NUMBER            PIC 9(7).                      
               10  CERTIFICATE-PREFIX            PIC X(3).                      
               10  CHECK-NUMBER                  PIC 9(13).                     
               10  FUND-FROM-TO                  PIC 9(7).                      
               10  TERRITORY                     PIC 9(2).                      
               10  FINCL-INST-NAME-1             PIC X(35).                     
               10  CLIENT-EXTERNAL-CODE          PIC 9(3).                      
               10  CASH-SUPER-SHEET-GROUP        PIC 9(3).                      
               10  SHARE-SUPER-SHEET-GROUP       PIC 9(3).                      
               10  PRINT-GROUP-ID                PIC 9(3).                      
               10  PRINT-LINE-ID                 PIC 9(3).                      
               10  MATRIX-ERROR-CD               PIC X(1).                      
                   88  MATRIX-ERROR-VOL-TRANSACTION  VALUE '1'.                 
                   88  MATRIX-ERROR-ORDER-PCS-TR     VALUE '2'.                 
                   88  MATRIX-ERROR-CASH-SUPER-GROUP VALUE '3'.                 
                   88  MATRIX-ERROR-SHARES-SUPER-GRP VALUE '4'.                 
                   88  MATRIX-ERROR-TR-SHEET-GROUP   VALUE '5'.                 
               10  CASH-CONTROL-RECONCILEMENT-CD PIC X(1).                      
                   88  CASH-CONTROL-RECONCILEMENT-YES                           
                                                     VALUE 'Y'.                 
                   88  CASH-CONTROL-RECONCILEMENT-NO VALUE 'N'.                 
               10  ACCOUNT-FROM-TO               PIC 9(11).                     
               10  FUND-GRP-NUMBER               PIC 9(7).                      
               10  LOI-FUND-GROUP-NUMBER         PIC 9(7).                      
               10  CONFIRM-DT.                                                  
                   15  CONFIRM-YYYY-DT           PIC 9(4).                      
                   15  CONFIRM-MM-DT             PIC 9(2).                      
                   15  CONFIRM-DD-DT             PIC 9(2).                      
               10  SHR-ACCT-CITIZEN-ST-CNTRY-CDE PIC 9(3).                      
               10  PAYMENT-METHOD-CDE            PIC X(1).                      
               10  SHR-ACCT-PPA-CONTROLLED-CDE   PIC X(1).                      
                   88  SHR-ACCT-PPA-CONTROLLED-YES   VALUE 'Y'.                 
                   88  SHR-ACCT-PPA-CONTROLLED-NO    VALUE 'N'.                 
               10  DEALER-CONTROL-LEVEL-CODE     PIC 9(1).                      
                   88  DEALER-CONTROL-NON-NETWORKING VALUE 0.                   
                   88  DEALER-CONTROL-BRKR-MARGIN-IRA                           
                                                     VALUE 1.                   
                   88  DEALER-CONTROL-CUST-NAME-BRKR VALUE 2.                   
                   88  DEALER-CONTROL-BROKERAGE-BRKR VALUE 3.                   
                   88  DEALER-CONTROL-BROKERAGE-FUND VALUE 4.                   
               10  PRE-POST-NOON                 PIC 9(1).                      
                   88  POST-NOON                     VALUE 0.                   
                   88  PRE-NOON                      VALUE 1.                   
               10  RECON-REQ-CDE                 PIC X(1).                      
                   88  RECON-REQ-YES                 VALUE 'Y'.                 
                   88  RECON-REQ-NO                  VALUE 'N'.                 
               10  REVERSE-MANUAL-ESTIMATE-CD    PIC X(1).                      
                   88  REVERSE-MANUAL-ESTIMATE-YES   VALUE 'Y'.                 
                   88  REVERSE-MANUAL-ESTIMATE-NO    VALUE 'N'.                 
               10  TRAN-STATUS-CDE               PIC X(1).                      
                   88  TRAN-STATUS-ACTIVE            VALUE 'A'.                 
                   88  TRANS-STATUS-BOGUS            VALUE 'B'.                 
                   88  TRANS-STATUS-EXTRACTED        VALUE 'E'.                 
                   88  TRANS-STATUS-DELETED          VALUE 'D'.                 
                   88  TRANS-STATUS-PENDING          VALUE 'P'.                 
                   88  TRANS-STATUS-REJECTED         VALUE 'R'.                 
                   88  TRANS-STATUS-REPROCESSED      VALUE 'S'.                 
                   88  TRANS-STATUS-WAREHOUSED       VALUE 'W'.                 
                   88  TRANS-STATUS-PROCESSED-NIGHTLY                           
                                                     VALUE 'N'.                 
               10  FIN-TR-ADVANCED-COMMISSION-AT PIC S9(13)V9(2)                
                    SIGN TRAILING SEPARATE.                                     
               10  FILLER                        PIC X(101).                    
           05  TRAILER-RECORD-AREA REDEFINES HEADER-RECORD-AREA.                
               10  RECORD-CT                     PIC 9(15).                     
               10  FILLER                        PIC X(454).         
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.			   
                   
             
