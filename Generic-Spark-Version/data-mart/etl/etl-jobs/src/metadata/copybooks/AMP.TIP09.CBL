       IDENTIFICATION DIVISION.
       PROGRAM-ID. ACCOUNT-MASTER-POSITION.
	     AUTHOR. DST. MODIFIED BY SHREENI.
	     DATE WRITTEN. 07/01/2009.
	     DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	     SELECT ACCOUNT-MASTER-POSITION ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  ACCOUNT-MASTER-POSITION.
       01  ACCOUNT-MASTER-POSITION.                                                             
           05  RECORD-CODE                       PIC X(3).                      
               88  NEW-ACCOUNT-ACTIVITY              VALUE 'NAA'.               
               88  ACCOUNT-MASTER-POSITION           VALUE 'AMP'.               
               88  NON-FINANCIAL-ACTIVITY            VALUE 'NFA'.               
               88  RECORD-REQUEST-HEADER             VALUE 'RHR'.               
               88  RECORD-REQUEST-TRAILER            VALUE 'RTR'.               
           05  SEQUENCE-NUMBER                   PIC 9(3).                      
           05  DETAIL-RECORD-1.                                                 
               10  FINS-ID                       PIC 9(7).                      
               10  DEALER-BRANCH-ID              PIC X(9).                      
               10  CUSIP-NUMBER                  PIC X(9).                      
               10  FUND-CODE                     PIC 9(7).                      
               10  CUSTOMER-ACCOUNT-NUMBER       PIC X(20).                     
               10  ACCOUNT-NUMBER-CODE           PIC X(1).                      
                   88  ACCOUNT-NUMBER-FUND           VALUE 'F'.                 
                   88  ACCOUNT-NUMBER-EXTERNAL       VALUE 'E'.                 
               10  ESTABLISHED-DATE.                                            
                   15  ESTABLISHED-YYYY-DATE.                                   
                       20  ESTABLISHED-CC-DATE   PIC 9(2).                      
                       20  ESTABLISHED-YY-DATE   PIC 9(2).                      
                   15  ESTABLISHED-MM-DATE       PIC 9(2).                      
                   15  ESTABLISHED-DD-DATE       PIC 9(2).                      
               10  LAST-MAINTENANCE-DATE.                                       
                   15  LAST-MAINTENANCE-YYYY-DATE.                              
                       20  LAST-MAINTENANCE-CC-DATE PIC 9(2).                      
                       20  LAST-MAINTENANCE-YY-DATE PIC 9(2).                      
                   15  LAST-MAINTENANCE-MM-DATE  PIC 9(2).                      
                   15  LAST-MAINTENANCE-DD-DATE  PIC 9(2).                      
               10  LINE-CODE                     PIC 9(1).                      
               10  ALPHA-CODE                    PIC X(10).                     
               10  DEALER-CONTROL-LEVEL-CODE     PIC X(1).                      
                   88  NON-NETWORKED-BROKER-ACCOUNT  VALUE '0'.                 
                   88  BROKER-CONTROL-IRA-MARGIN-ACCT VALUE '1'.                 
                   88  CUSTOMER-NAME-BROKERAGE-ACCT  VALUE '2'.                 
                   88  BROKER-CONTROL-BROKERAGE-ACCT VALUE '3'.                 
                   88  FUND-CONTROLLED-BROKERAGE-ACCT VALUE '4'.                 
               10  SOCIAL-CODE                   PIC 9(3).                      
               10  RESIDENT-STATE-COUNTRY        PIC 9(3).                      
               10  TAX-IDENTIFICATION-NUMBER     PIC 9(9).                      
               10  SS-CODE                       PIC 9(1).                      
                   88  SS-MISSING                    VALUE 0.                   
                   88  SSN-NON-CERTIFIED             VALUE 1.                   
                   88  SSN-CERTIFIED                 VALUE 2.                   
                   88  TIN-NON-CERTIFIED             VALUE 3.                   
                   88  TIN-CERTIFIED                 VALUE 4.                   
                   88  SS-APPLIED                    VALUE 5.                   
                   88  SSN-IRS-OFFENDER              VALUE 6.                   
                   88  TIN-IRS-OFFENDER              VALUE 7.                   
                   88  SSN-ACCOUNT-TYPE-EXEMPT       VALUE 8.                   
                   88  TIN-ACCOUNT-TYPE-EXEMPT       VALUE 9.                   
               10  SWP-ACCOUNT                   PIC 9(1).                      
                   88  NOT-SWP                       VALUE 0.                   
                   88  YES-SWP                       VALUE 1.                   
               10  PRE-AUTH-CK-ACCT              PIC 9(1).                      
                   88  NOT-PRE-AUTH                  VALUE 0.                   
                   88  YES-PRE-AUTH                  VALUE 1.                   
               10  ACH-FLAG                      PIC 9(1).                      
                   88  ACH-NO                        VALUE 0.                   
                   88  ACH-YES                       VALUE 1.                   
               10  REINVEST-TO-FUND              PIC 9(1).                      
                   88  REINVEST-TO-FUND-NO           VALUE 0.                   
                   88  REINVEST-TO-FUND-YES          VALUE 1.                   
               10  CAP-GAINS                     PIC 9(1).                      
                   88  CG-REINVEST                   VALUE 0.                   
                   88  CG-CASH                       VALUE 1.                   
               10  DIVIDEND-1                    PIC 9(1).                      
                   88  DIV-1-REIN                    VALUE 0.                   
                   88  DIV-1-CASH                    VALUE 1.                   
               10  CHECK-WRITING-ACCT            PIC 9(1).                      
                   88  NOT-CWR                       VALUE 0.                   
                   88  YES-CWR                       VALUE 1.                   
               10  EXPEDITED-ACCT                PIC 9(1).                      
                   88  NOT-EXR                       VALUE 0.                   
                   88  YES-EXR                       VALUE 1.                   
               10  SUB-ACCOUNTING                PIC 9(1).                      
                   88  NOT-SUB-ACCT                  VALUE 0.                   
                   88  YES-SUB-ACCT                  VALUE 1.                   
               10  FOREIGN-TAX-RATE              PIC V9(3).                     
               10  ZIP-CODE.                                                    
                   15  ZIP-DELIVERY-OFFICE-CODE  PIC 9(5).                      
                   15  ZIP-CODE-SUFFIX.                                         
                       20  ZIP-SECTOR-CODE       PIC 9(2).                      
                       20  ZIP-SEGMENT-CODE      PIC 9(2).                      
               10  ZIP-CODE-FUTURE               PIC 9(2).                      
               10  CUMULATIVE-DISCOUNT-NO        PIC 9(9).                      
               10  LOI-NUMBER                    PIC 9(9).                      
               10  TIMER-FLAG                    PIC 9(1).                      
                   88  TIMER-NO                      VALUE 0.                   
                   88  TIMER-YES                     VALUE 1.                   
               10  LISTB-FLAG                    PIC 9(1).                      
                   88  LISTB-NO                      VALUE 0.                   
                   88  LISTB-YES                     VALUE 1.                   
               10  MONITOR-VIP-FLAG              PIC 9(1).                      
                   88  MONITOR-VIP-NO                VALUE 0.                   
                   88  MONITOR-VIP-YES               VALUE 1.                   
               10  EXPEDITED-EXCHANGE            PIC 9(1).                      
                   88  NOT-EXPEDITED                 VALUE 0.                   
                   88  YES-EXPEDITED                 VALUE 1.                   
               10  PENALTY-WITHHOLDING           PIC 9(1).                      
                   88  NOT-WITHHOLDING               VALUE 0.                   
                   88  YES-WITHHOLDING               VALUE 1.                   
               10  ISSUE-CODE                    PIC 9(1).                      
                   88  NO-CERTIFICATE                VALUE 0.                   
                   88  YES-CERTIFICATE               VALUE 1.                   
               10  STOP-TRANSFER-CODE            PIC 9(1).                      
                   88  NO-STOP-TRANSFER              VALUE 0.                   
                   88  YES-STOP-TRANSFER             VALUE 1.                   
               10  BLUE-SKY-EXEMPT               PIC 9(1).                      
                   88  NOT-EXEMPT                    VALUE 0.                   
                   88  YES-EXEMPT                    VALUE 1.                   
               10  DEBIT-CARD                    PIC 9(1).                      
                   88  NO-DEBIT                      VALUE 0.                   
                   88  YES-DEBIT                     VALUE 1.                   
               10  FIDUCIARY-ACCOUNT             PIC 9(1).                      
                   88  NO-FIDUCIARY                  VALUE 0.                   
                   88  YES-FIDUCIARY                 VALUE 1.                   
               10  PLAN-STATUS-CODE              PIC 9(1).                      
                   88  OPEN-ACCOUNT                  VALUE 0.                   
                   88  CLOSED-ACCOUNT                VALUE 1.
               10  NAV-CDE                       PIC X(1).                      
                   88  NAV-NO                        VALUE '0'.                 
                   88  NAV-YES                       VALUE '1' THRU             
           '9'.                                      
                   88  NAV-YES                       VALUE 'A' THRU             
           'Z'.
               10  MAILING-FLAG                  PIC 9(1).                      
                   88  NO-MAIL-FLAG                  VALUE 0.                   
                   88  YES-MAIL-FLAG                 VALUE 1.                   
               10  INTERESTED-PARTY-CDE          PIC X(1).                      
                   88  INTERESTED-PARTY-YES          VALUE 'Y'.                 
                   88  INTERESTED-PARTY-NO           VALUE 'N'.                 
               10  SHR-ACCT-PHONE-CHK-REDEMPT-CDE PIC X(1).                      
                   88  SHR-ACCT-PHONE-CHK-REDEMPT-YES VALUE 'Y'.                 
                   88  SHR-ACCT-PHONE-CHK-REDEMPT-NO VALUE 'N'.                 
               10  SHR-ACCT-HOUSE-ACCOUNT-CDE    PIC X(1).                      
                   88  SHR-ACCT-HOUSE-ACCOUNT-YES    VALUE 'Y'.                 
                   88  SHR-ACCT-HOUSE-ACCOUNT-NO     VALUE 'N'.                 
           05  DETAIL-RECORD-2 REDEFINES DETAIL-RECORD-1.                       
               10  DIVIDEND-MAIL                 PIC 9(1).                      
                   88  NO-DVND-MAIL                  VALUE 0.                   
                   88  YES-DVND-MAIL                 VALUE 1.                   
               10  STOP-PURCHASE                 PIC 9(1).                      
                   88  NO-RESTRICT                   VALUE 0.                   
                   88  YES-RESTRICT                  VALUE 1.                   
               10  STOP-MAIL                     PIC 9(1).                      
                   88  NO-STOP-MAIL                  VALUE 0.                   
                   88  YES-STOP-MAIL                 VALUE 1.                   
               10  FRACTIONAL-CHECK              PIC 9(1).                      
                   88  NO-CHECK                      VALUE 0.                   
                   88  YES-CHECK                     VALUE 1.                   
               10  REGISTRATION-LINE-1-TEXT      PIC X(35).                     
               10  REGISTRATION-LINE-2-TEXT      PIC X(35).                     
               10  REGISTRATION-LINE-3-TEXT      PIC X(35).                     
               10  REGISTRATION-LINE-4-TEXT      PIC X(35).                     
               10  SHR-ACCT-CLIENT-DEFINED-1-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-1-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-1-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-2-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-2-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-2-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-3-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-3-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-3-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-4-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-4-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-4-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-5-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-5-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-5-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-6-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-6-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-6-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-7-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-7-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-7-NO  VALUE 'N'.                 
               10  SHR-ACCT-CLIENT-DEFINED-8-CDE PIC X(1).                      
                   88  SHR-ACCT-CLIENT-DEFINED-8-YES VALUE 'Y'.                 
                   88  SHR-ACCT-CLIENT-DEFINED-8-NO  VALUE 'N'.                 
               10  SHR-ACCT-PRICE-SCHED-CDE      PIC 9(1).                      
                   88  SHR-ACCT-PRICE-SCHED-REDUCED  VALUE 1.                   
                   88  SHR-ACCT-PRICE-SCHED-REGULAR  VALUE 2.                   
               10  SHR-ACCT-MGMT-CO-EMPLOYEE-CDE PIC X(1).                      
                   88  SHR-ACCT-MGMT-CO-EMPLOYEE-YES VALUE 'Y'.                 
                   88  SHR-ACCT-MGMT-CO-EMPLOYEE-NO  VALUE 'N'.                 
           05  DETAIL-RECORD-3 REDEFINES DETAIL-RECORD-1.                       
               10  REGISTRATION-LINE-5-TEXT      PIC X(35).                     
               10  REGISTRATION-LINE-6-TEXT      PIC X(35).                     
               10  REGISTRATION-LINE-7-TEXT      PIC X(35).                     
               10  REPRESENTATIVE-NUMBER         PIC X(9).                      
               10  REPRESENTATIVE-NAME           PIC X(30).                     
               10  SHR-ACCT-CLIENT-DEFINED-1-TXT PIC X(10).                     
           05  DETAIL-RECORD-4 REDEFINES DETAIL-RECORD-1.                       
               10  FINS-ACCOUNT-ID               PIC X(20).                     
               10  FINS-ACCOUNT-CDE              PIC X(1).                      
                   88  FINS-ACCOUNT-FUND             VALUE 'F'.                 
                   88  FINS-ACCOUNT-EXTERNAL         VALUE 'E'.                 
               10  PRIMARY-INVESTOR-PHONE-NBR    PIC 9(15).                     
               10  SECONDARY-INVESTOR-PHONE-NBR  PIC 9(15).                     
               10  TRUSTEE-NSCC-FIRM-NBR         PIC X(4).                      
               10  NSCC-TPA-FIRM-NBR             PIC X(4).                      
               10  SHR-ACCT-CITIZEN-ST-CNTRY-CDE PIC 9(3).                      
               10  EXT-TRACKING-ID               PIC X(20).                     
               10  CUSTODIAN-ID                  PIC 9(7).                      
               10  THIRD-PRTY-ADMN-ID            PIC 9(7).                      
               10  EXTERNAL-PLAN-ID              PIC X(9).                      
               10  TOTAL-SHARE-CT                PIC 9(11)V9(4).                
               10  ACCOUNT-ORIGINATION-ID        PIC 9(3).                      
               10  AFL-FI-ID                     PIC 9(7).                      
               10  AFL-FI-BR-ID                  PIC X(9).                      
               10  AFL-FI-BR-REP-ID              PIC X(9).                      
               10  AFL-FI-PCS-ROL-CD             PIC X(1).                      
                   88  AFL-FI-PCS-ROL-CLEARING       VALUE 'C'.                 
                   88  AFL-FI-PCS-ROL-EXECUTING      VALUE 'E'.                 
                   88  AFL-FI-PCS-ROL-NA             VALUE 'Z'.                 
               10  FILLER                        PIC X(5).                      
           05  DETAIL-RECORD-5 REDEFINES DETAIL-RECORD-1.                       
               10  FUND-CODE                     PIC 9(7).                      
               10  ACCOUNT-NUMBER                PIC 9(11).                     
               10  FIRM-ACCOUNT-NBR              PIC X(20).                     
               10  SEC-ISS-ID                    PIC X(9).                      
               10  TICKER-SYMBOL-ID              PIC X(5).                      
               10  TA-MUTUAL-FUND-ID             PIC X(20).                     
               10  EXTL-OMB-MFA-NBR              PIC X(20).                     
               10  OMB-FIRM-ACCT-NBR             PIC X(20).                     
               10  TOTAL-CURRENT-CRG-AT          PIC 9(15)V9(2).                
               10  FILLER                        PIC X(25).                     
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.
