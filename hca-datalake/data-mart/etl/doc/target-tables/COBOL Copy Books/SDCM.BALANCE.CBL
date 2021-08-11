       IDENTIFICATION DIVISION.
       PROGRAM-ID. SDCM-BALANCING-DATA.
	   AUTHOR. DST. MODIFIED BY SHREENI.
	   DATE WRITTEN. 07/01/2009.
	   DATE COMPILED. 07/01/2009.
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       FILE-CONTROL.
	    SELECT SDCM-BALANCING-DATA ASSIGN TO "FNAME".
       DATA DIVISION.
       FILE SECTION.
       FD  SDCM-BALANCING-DATA.
       01  SDCM-BALANCING-DATA.                                                             
           05  SYSTEM-ID                         PIC X(3).                      
           05  SUPER-SHEET-DTE.                                                 
               10  SUPER-SHEET-YYYY-DTE.                                        
                   15  SUPER-SHEET-CC-DTE        PIC 9(2).                      
                   15  SUPER-SHEET-YY-DTE        PIC 9(2).                      
               10  SUPER-SHEET-MM-DTE            PIC 9(2).                      
               10  SUPER-SHEET-DD-DTE            PIC 9(2).                      
           05  RECORD-TYPE-CDE                   PIC X(2).                      
               88  RECORD-TYPE-HEADER                VALUE 'HH'.                
               88  RECORD-TYPE-DETAIL                VALUE 'DD'.                
               88  RECORD-TYPE-TRAILER               VALUE 'TT'.                
           05  DETAIL-TRANSMIT-AREA.                                            
               10  FUND-CODE                     PIC 9(7).                      
               10  SUBSCRIPTION-DOLLAR-AMT       PIC 9(15)V9(2).                
               10  REDEMPTION-DOLLAR-AMT         PIC 9(15)V9(2).                
               10  DIVIDEND-ACCRUAL-AMT          PIC 9(15)V9(2).                
               10  SHARES-OUTSTANDING-AMT        PIC 9(13)V9(4).                
               10  RECORD-DATE-SHARES-AMT        PIC 9(13)V9(4).                
               10  DAILY-ACCRUAL-CDE             PIC X(1).                      
                   88  DAILY-ACCRUAL-POSITIVE        VALUE 'P'.                 
                   88  DAILY-ACCRUAL-NEGATIVE        VALUE 'N'.                 
               10  DAILY-ACCRUAL-AMT             PIC 9(15)V9(2).                
               10  DIVIDEND-CLOSE-OUT-CDE        PIC X(1).                      
                   88  DIVIDEND-CLOSE-OUT-POSITIVE   VALUE 'P'.                 
                   88  DIVIDEND-CLOSE-OUT-NEGATIVE   VALUE 'N'.                 
               10  DIV-CLOSE-OUT-AMT             PIC 9(15)V9(2).                
               10  DAILY-ADJUST-CDE              PIC X(1).                      
                   88  DAILY-ADJUST-POSITIVE         VALUE 'P'.                 
                   88  DAILY-ADJUST-NEGATIVE         VALUE 'N'.                 
               10  DAILY-ADJUST-AMT              PIC 9(15)V9(2).                
               10  CERTIFICATE-SHR-AMT           PIC 9(13)V9(4).                
               10  NON-CERTIFICATE-SHR-AMT       PIC 9(13)V9(4).                
               10  FAIL-FILE-SALE-AMT            PIC 9(13)V9(4).                
               10  FREE-FILE-SALE-AMT            PIC 9(13)V9(4).                
               10  REDEMPTION-FAIL-AMT           PIC 9(13)V9(4).                
               10  DIVIDEND-ON-OFF-CDE           PIC X(1).                      
                   88  DIVIDEND-ON-OFF-POSITIVE      VALUE 'P'.                 
                   88  DIVIDEND-ON-OFF-NEGATIVE      VALUE 'N'.                 
               10  DIV-ON-OFF-AMT                PIC 9(15)V9(2).                
               10  NSCC-ON-OFF-CDE               PIC X(1).                      
                   88  NSCC-ON-OFF-POSITIVE          VALUE 'P'.                 
                   88  NSCC-ON-OFF-NEGATIVE          VALUE 'N'.                 
               10  NSCC-ON-AMT                   PIC 9(15)V9(2).                
               10  NAV-PER-SHARE                 PIC 9(5)V9(4).                 
               10  FILLER                        PIC X(11).                     
           05  TRAILER-TRANSMIT-AREA REDEFINES DETAIL-TRANSMIT-AREA.            
               10  TOTAL-TRANS-RECS-OUT-NBR      PIC 9(9).                      
               10  FILLER                        PIC X(278).                    
       WORKING-STORAGE SECTION.
       PROCEDURE DIVISION.
        STOP RUN.			   
                  
