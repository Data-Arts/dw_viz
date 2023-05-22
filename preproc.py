import pandas as pd
import numpy as np

numfields=['ADMISS','ADMISSRS','ADMISSTO','ADULTATND','ADVERTCD','ADVERTRS','ADVERTTO','ADVOCENG','ADVPRDEV','ADVPRGA','ADVPRPRG','ADVPRSF','ADVPRTOT','AFTA','ALLEDFR','ALLEDPD','APPFEECD','ARAG','ARTSALCD','AUDIT','BDCASH','BDCORPUS','BDENDDRW','BDENDINV','BDMAXDRW','BDOTH','BENSF','BENTOT','BOARDCD','BOARDHRS','BONDPYCUR','BONDPYLT','CAPEXPTOT','CAPPM','CAPTO','CAPUN','CASHAG','CASHCD','CASRESRVTOT','CDPTAXON','CITYPM','CITYCD','CITYNARCD','CITYNCD','CITYSF','CITYTO','CLIAOTAG','CLMGCNDEV','CLMGCNGA','CLMGCNPRG','CLMGCNTOT','CNCESSCD','CNCESSRS','CNCESSTO','CNFHSPDEV','CNFHSPGA','CNFHSPPRG','CNFHSPSF','CNFHSPTOT','CNTART','CNTPMCD','CNTOTPMCD','CNTOTTOCD','CNTOTUNCD','CNTYNARCD','COMCNSGN','CONSTRUCT','CONSULTHIFEE','CONSULTLOFEE','CONSULTN','CONSVC','CONSVCRS','CONSVCTO','CORPCD','CORPNCD','CORPPM','CORPSF','CORPTO','COUNTYCD','COUNTYNCD','COUNTYPM','COUNTYSF','COUNTYTO','CRLNEOY','CRLNLIM','CRPNARCD','CURASSAG','CURLIAAG','CYear','CYRFOUND','CZIP','CZIP4','DEFREVAG','DEPRCNCD','DEVBENCD','DEVCONTO','DEVSALCD','DIGADS','DIGSHOW','DMAILN','DPRCNDEV','DPRCNGA','DPRCNPRG','DPRCNSF','DPRCNTO','DUESDEV','DUESGA','DUESPRG','DUESSF','DUESTOT','DUNS','ED3INCCDN','ED3INCRS','ED3INCTON','EDPERFS','EDPUBPROF','EDSERIES','EDSERPERFS','EIN','EMAILN','EQUIPCD','ERNNCAPCD','EXHEXPDEV','EXHEXPGA','EXHEXPPRG','EXHEXPTOT','EXHWKSTOT','FBFLWRS','FEDCD','FEDNARCD','FEDNCD','FEDPM','FEDSF','FEDTO','FESTATTOT','FESTSHOW','FISCSPEIN','FILMS','FILMSCRN','FIXDASSAG','FIXDASSCD','FIXDASSPM','FIXDASSUN','FLKFLWRS','FNDNARCD','FOUNDCD','FOUNDNCD','FOUNDPM','FOUNDSF','FOUNDTO','FRATNDCD','FRATNDSF','FSAFCD','FSAFRS','FSAFTO','FTACARCD','FTARTPER','FTEMPS','FTSEAS','FTSEASFTE','FTVOLS','FYEND','GA1099','GABENCD','GALSALTM','GALSALTO','GALSALUN','GASAL','GDTOURRS','GDTOURTO','GDTOURUN','GFTSLS','GFTSLSRS','GFTSLSTO','GOVCD','GOVN','GOVPM','GOVTO','GPFLWRS','GRANTDEV','GRANTGA','GRANTPRG','GRANTSF','GRANTTOT','GRPINRS','GRPINTO','GRPINUN','GUIDTPERF','GUIDTSHOW','HASADVO','HASAFTA','HASAHC','HASARTSED','HASAUDIT','HASBCAST','HASBHS','HASCOMM','HASCOMP','HASCONF','HASDEVS','HASENDW','HASEXHS','HASFESTS','HASFS','HASGRNTS','HASLAO','HASLF','HASMEDSUB','HASMEMBR','HASNGLD','HASNONOPE','HASNONOPR','HASOPAM','HASOPREH','HASPERFS','HASPUB','HASRESIDENC','HASRESRCH','HASRSREV','HASSCREENS','HASSF','HASSUBEVS','HASTOURS','HISUBCD','HITIXMUS','HITIXPERF','HITXIMEMB','HITXOMEMB','ICART','ICTOT','IGFLWRS','IKB','IKCAO','IKCE','IKCIP','IKL','IKLBI','IKO','INDMEMLPSN','NewMembs','INKIND','INKINDOPPM','INKINDOPTO','INKINDOPUN','INKINDPM','INKINDSF','INKINDTM','INKINDTO','INKNDNOPPM','INKNDNOPTO','INKNDNOPUN','INSURCD','INSURDEV','INSURGA','INSURPRG','INSURSF','INTEXDEV','INTEXGA','INTEXPRG','INTEXSF','INTEXTOT','INTFXDASS','INTNTOT','INVAG','INVRESRVTOT','IRSDATE','KIDATTND','KIDATTPSF','KIDSINSCHLS','LECOCC','LOANINCD','LOANINRS','LOANINTO','LOANSCURAG','LOANSCURPM','LOANSCURTO','LOANSCURUN','LOANSLTAG','LOANSLTPM','LOANSLTTO','LOANSLTUR','LOCART','LOCARTSF','LOCARTTOT','LOCPREM','LOCPREMEXH','LOCPREMPRF','LOSUBCD','LOTIXIMEMB','LOTIXMUS','LOTIXOMEMB','LOTIXPERF','LTASSAG','LTASSSF','LTINVAG','LTLIAAG','LTLIACD','LTLIAPM','LTLIAUN','MBRINC','MBRINCTO','MBRINDFREN','MBRINDTOTN','MBRINRS','MBRORGFRN','MBRORGPDN','MBRORGTOTN','MBROTHFRN','MBROTHPDN','MBROTHTOTN','MEMBERS','MEMRETRNN','MKTPFO','MKTSAT','MKTTOT','MKTTOTSF','MOCOUNT','MORTCUR','MORTLT','NARTRCD','NARTRCDX','NARTRPM','NARTRTO','NATPREM','NATPRMEXH','NATPRMPRF','NETINCCD','NETINCNOP','NETINCOP','NETINCSF','NETINCUN','NewSubs','NISPID','NLOCART','NLOCARTSF','NLOCARTTOT','NOAUDITAG','NOCOMMISS','NOLECTUR','NONOPEXPTOT','NONOPSURP','NOOTPRG','NOTOPPM','NOTOPTO','NOTOPUN','NOTOPXFER','NOTOPXFERPM','NOTOPXFERTM','NOTOPXFERTO','NPERSEXDEV','NPERSEXGA','NPERSEXPRG','NPERSEXTOT','NVSTINOPCD','NVSTINOPPM','NVSTINOPTO','NVSTINOTCD','NVSTINOTPM','NVSTINOTTO','OACTSAT','OCCDEV','OCCGA','OCCPROG','OCCSF','OCCTOT','OCNTART','OCOMCNSGN','OFFCDEV','OFFCGA','OFFCPRG','OFFCTOT','OFFEDSHW','OFFSF','OFXDASS','ONONOPEXDEV','ONONOPEXGA','ONONOPEXPRG','OOPEXDEV','OOPEXGA','OOPEXPRG','OOPEXSF','OOPEXTOT','OPEARNPM','OPEXPTOT','OPPM','OPREHTOT','OPRGRVCD','OPRGRVTM','OPRGRVTO','OPSF','OPSURPCDD','OPTO','OPUN','OrgsID','ORGLAT','ORGLONG','ORGMEMLPSN','ORGMEMNEWN','ORGMEMRETRN','OrgYRFOUND','OSMFLWRS','OTARTTOT','OTCURASAG','OTDON','OTDONPM','OTDONTO','OTHEARNCD','OTHEARNRS','OTHEARNSF','OTHEARNTO','OTHERMKTG','OTHINDCD','OTHINDPM','OTHINDSF','OTHINDTO','OTHLNCUR','OTHLNLT','OTHMEMLPSN','OTHMEMNEWN','OTMEMRETRNN','OTHNARCD','OTHRENTRS','OTHRENTTO','OTHRENTUN','OTINDNCD','OTLTASSAG','OTPMCUR','OTPRGPRF','OTRESRVTOT','OTTOCUR','OTUNCUR','PACTSAT','PARK','PARKRS','PARKTO','PAYABAG','PAYABPM','PAYABTO','PAYABUN','PCNTART','PCOMCNSGN','PDACARCD','PDATNDADM','PDATNDPERF','PEIN','PERSNEXDEV','PERSNEXGA','PERSNEXPRG','PERSNEXSF','PFEEFUND','PFEEGA','PFEEPRG','PFEESF','PFEETOT','PHATTTOT','PINFLWRS','PLGRAG','PLGRCURTOT','PLGRTLTAG','PLGRTLTTO','PMACEXP','PMARECVCD','PMCASH','PMCASHTO','PMCLIAOT','PMCORPUS','PMCURASS','PMCURINV','PMCURLIA','PMDEFREV','PMENDDRW','PMEXSHOW','PMIFBALA','PMIFBALL','PMINVTO','PMLTASS','PMLTINV','PMLTLIAOT','PMMAXDRW','PMOTHASS','PMOTHTOT','PMPLGRCUR','PMPLGRTLT','PMPMCASH','PMPMEND','PMPMINV','PMPMOTH','PMPPE','PMTOTASS','PNOEXDEV','PNOEXGA','PNOEXPRG','PNOEXTOT','PPADJPM','PPADJTO','PPADJUN','PREPADAG','PREPADCD','PRFARTTOT','PRGBENCD','PRODEXDEV','PRODEXGA','PRODEXPRG','PRODEXTOT','PRRADTVADS','PRSFEERS','PRSFEVTTO','PRTSHDEV','PRTSHGA','PRTSHPRG','PRTSHSF','PRTSHTOT','PRVEXPERTS','PRVSPACE','PTARTPER','PTEMPS','PTFTES','PTSEAS','PTSEASFTE','PTVFTES','PTVOLS','PUBDIST','PUBPRINT','PUBSALTM','PUBSALTO','PUBSALUN','PVTLESSONS','PVTSTUDS','RECDEV','RECGA','RECPRG','RECTOT','RESRPTS','RESRVDRW','RESRVMAXDR','RESRVTOT','ROYALCD','ROYALRS','ROYALTO','RYLDEV','RYLEXPCD','RYLGA','RYLTOT','SALSF','SALTO','SENATTND','SHLTRCD','SHLTRYRFND','SHLTREIN','SMORG','SMEDFLWRS','SNONOPEXDV','SNONOPEXGA','SNONOPEXPR','SomeDate','SOPEXDEV','SOPEXGA','SOPEXPRG','SOPEXSF','SPC1SQFT','SPC1ZIP','SPC2SQFT','SPC2ZIP','SPC3SQFT','SPC3ZIP','SPC4SQFT','SPC4ZIP','SPC5SQFT','SPC5ZIP','SPCRENTRS','SPCRENTTO','SPCRENTUN','SPEVCRPN','SPEVGRPM','SPEVGRTO','SPEVGRUN','SPEVINDN','SPEVNETPM','SPEVNETTO','SPEVNETUN','SPEVOTHN','SPEVTOTN','SPNSOR','SPNSORRS','SPNSORTO','SPRENTSQFT','STANARCD','STATECD','STATENCD','STATEPM','STATESF','STATETO','STINCRS','STINCTO','STXINCUN','SUBATTF','SUBATTP','SUBBCSTCD','SUBBCSTRS','SUBBCSTTO','SUBINCFLRS','SUBINCFLTO','SUBINCFLX','SUBINCFUL','SUBINCFXRS','SUBINCFXTO','SUBMEDBFR','SUBMEDBPD','SUBMEDBTO','SUBMEDPFR','SUBMEDPPD','SUBMEDPTO','SUBPUBCD','SUBPUBRS','SUBPUBTO','SUBRETRNN','SUBSTATTOT','SUGDONAMT','TCNTCDOPSF','TKTADMFR','TKTADMPD','TKTADMTO','TMCARESRV','TMCASHTO','TMCORPUS','TMENDBSCHK','TMENDDRW','TMEXSHOW','TMFLWRS','TMINVRESRV','TMINVTO','TMMAXDRW','TMOTHTOT','TMOTRESRV','TMRESRVTOT','TMTOTASS','TOATNDCD','TOATNDSF','TOCLIAOT','TOCURASS','TOCURINV','TODEFREV','TOLTINV','TOLTLIAAG','TOLTLIAOT','TONASSCURSF','TONASSLTSF','TONASSAG','TOOTHASS','TOPERFCD','TOPMEND','TOSHOWCD','TOT1099FM','TOT1099SF','TOTACEXP','TOTACEXPAG','TOTARECVCD','TOTASSAG','TOTASSETS','TOTBLDG','TOTCAPCD','TOTCMP','TOTCNTOPTO','TOTCNTTO','TOTCURCD','TOTDEVCD','TOTEXSF','TOTEXTOX','TOTGALF','TOTIMPRV','TOTLAND','TOTLIAAG','TOTLIACD','TOTLIAPM','TOTLIATM','TOTLIATOT','TOTLIAUN','TOTLOANSCD','TOTMEND','TOTPMIN','TOTPMX','TOTPRG','TOTSALCD','TOTSUBF','TOTSUBP','TOTTMIN','TOTTMX','TOTUNIN','TOTUNX','TOURISTSN','TOURREVCD','TRAVHTDEV','TRAVHTGA','TRAVHTPRG','TRAVHTSF','TRAVHTTOT','TREXSHOW','TRIBAL','TRIBALPM','TRIBALTO','TRIBNARCD','TRIBNCD','TRNCSTCD','TRNCSTWC','TRSNARCD','TRUSTCD','TRUSTNCD','TRUSTPM','TRUSTSF','TRUSTTO','TTEARNSF','TTNVSTCD','TTNVSTPM','TTNVSTTM','TTNVSTTO','TWFLWRS','UANARCD','UAOCDNEW','UAOPMNEW','UAOSF','UAOTONEW','UNACEXP','UNARECVCD','UNBDEND','UNCARESRV','UNCASH','UNCLIAOT','UNCURASS','UNCURINV','UNCURLIA','UNDEFREV','UNIFBALA','UNIFBALL','UNINVRESRV','UNLTASS','UNLTINV','UNLTLIAOT','UNOTHASS','UNOTRESRV','UNPLGRCUR','UNPLGRTLT','UNPMCASH','UNPMEND','UNPMINV','UNPMOTH','UNPPE','UNRESRVTOT','UNTOTASS','UWEBVIS','VACTSAT','VATTTOT','VCNTART','VCOMCNSGN','VFATND','VISARTTOT','VMFLWRS','VPTOATND','WEBVIEWS','WEBVISITS','WKSDEVEL','WKSRDG','WorldCD','WORLDEXH','WORLDPRF','XFERS','XFERSPM','XFERSTO','YRINC','YOUTFLWRS']

numfields_st=['DEVCONTO', 'GA1099', 'CNTART', 'TOT1099SF', 'TOT1099FM',
       'TOT1099', 'APACEXPAG', 'APACEXPRS', 'APACEXPTO', 'APACEXPUN',
       'KIDSINSCHLS', 'BOARDCD', 'BCSTSHOWLIVE', 'BCSTSHOWODEM',
       'BCSTPERFLIVE', 'HASBCAST', 'CASHAG', 'RSCASH', 'CASHCD', 'UNCASH',
       'NETINCCD', 'NETINCNOP', 'NETINCNOPRS', 'NONOPSURP', 'NETINCOP',
       'OPSURPRSD', 'OPSURPCDD', 'NOCOMMISS', 'COMPRGDIGLIVE',
       'COMPRGDIGODEM', 'COMPRGINP', 'COMPRGDIGPERF', 'COMPRGINPPERF',
       'COMPETS', 'TRUSTNCD', 'CORPNCD', 'FOUNDNCD', 'CITYNCD',
       'COUNTYNCD', 'FEDNCD', 'STATENCD', 'OTINDNCD', 'NCONTRIBS',
       'TRIBNCD', 'COVIDFUR', 'COVIDLO', 'COVIDRET', 'TOCURASS',
       'CURASSAG', 'RSCURASS', 'UNCURASS', 'CURASSUNLAG', 'CURASSUNLRS',
       'CURASSUNLTO', 'CURASSUNLUN', 'TOCLIAOT', 'TOTCURCD', 'CURLIAAG',
       'RSCURLIA', 'UNCURLIA', 'DEFREVAG', 'RSDEFREV', 'TODEFREV',
       'UNDEFREV', 'DPRCNDEV', 'DPRCNGA', 'DPRCNPRG', 'DPRCNSF',
       'DIGEXPTOT', 'PMEXDIGODEM', 'PMEXSHOWINP', 'TMEXDIGODEM',
       'TMEXSHOWINP', 'TREXDIGODEM', 'TREXSHOWINP', 'FESTSHOW',
       'FESCONFDIGLIVE', 'FESTCONFDIGODEM', 'FESCONFINP',
       'FESCONFEVNTSDIGLIVE', 'FESCONFEVNTSINP', 'FIELDTDIGLIVE',
       'FIELDTDIGODEM', 'FIELDTINP', 'FIELDTDIGPERF', 'FIELDTINPPERF',
       'FILMSPROD', 'FSPONSRAMT', 'FSPONSRCT', 'FIXDASSAG', 'FIXDASSRS',
       'FIXDASSCD', 'FIXDASSUN', 'FTEMPS', 'FTTURNCD', 'FTSEAS',
       'FTSEASTURN', 'DEVEXPERSF', 'GAEXPERSF', 'FXDASCDD',
       'GUIDTDIGLIVE', 'GUIDTDIGODEM', 'GUIDTINP', 'GUIDTDIGPERF',
       'GUIDTINPPERF', 'HASAUDIT', 'HASFS', 'HASLF', 'NOAUDITAG',
       'HASNONOPE', 'HASNONOPR', 'HASOPAM', 'SHLTRCD', 'HASRSREV',
       'HASSF', 'INKNDNOPTO', 'INKNDNOPRS', 'INKNDNOPUN', 'INKINDOPTO',
       'INKINDOPRS', 'INKINDOPUN', 'INKINDRS', 'INKIND', 'ICTOT',
       'INTEXDEV', 'INTEXGA', 'INTEXPRG', 'INTEXSF', 'INTNTOT',
       'NVSTINOTRS', 'NVSTINOTTO', 'NVSTINOTCD', 'NVSTINOPRS',
       'NVSTINOPTO', 'NVSTINOPCD', 'TTNVSTRS', 'TTNVSTTO', 'TTNVSTCD',
       'INVAG', 'RSCURINV', 'TOCURINV', 'UNCURINV', 'LTINVAG', 'RSLTINV',
       'TOLTINV', 'UNLTINV', 'LECTDIGLIVE', 'LECTDIGODEM', 'LECTINP',
       'LECTDIGPERF', 'LECTINPPERF', 'DEPRCNCD', 'TOTLIACD', 'TOTLIARS',
       'TOTLIAUN', 'CRLNLIM', 'VFATND', 'VPTOATND', 'VATTTOT',
       'LOANSCURAG', 'LOANSCURRS', 'LOANSCURTO', 'LOANSCURUN',
       'LOANSLTAG', 'LOANSLTRS', 'LOANSLTTO', 'LOANSLTUR', 'FBFLWRS',
       'IGFLWRS', 'OTHSMFLWRS', 'SCFLWRS', 'TTFLWRS', 'TWFLWRS',
       'VMFLWRS', 'YOUTFLWRS', 'DIRMKTGPS', 'MKTSATPS', 'MKTTOTPRE',
       'MKTTOT', 'HITXIMEMB', 'LOTIXIMEMB', 'HIITXIMEMB', 'LOITIXIMEMB',
       'MEMINDCOUNT', 'IMEMBRENEW', 'HITXOMEMB', 'LOTIXOMEMB',
       'HIITXOMEMB', 'LOITIXOMEMB', 'MEMORGCOUNT', 'OMEMBRENEW',
       'OTLTASSAG', 'RSOTHASSLT', 'TOOTHASSLT', 'UNOTHASSLT', 'TOLTASS',
       'LTASSAG', 'RSLTASS', 'UNLTASS', 'LTLIACD', 'TOLTLIAAG',
       'RSOTHLIALT', 'TOOTHLIALT', 'UNOTHLIALT', 'LTLIATO', 'LTLIAAG',
       'LTLIARS', 'LTLIAUN', 'ONONOPEXTOT', 'NOPREVOTHRS', 'NOPREVOTHTO',
       'NOPREVOTHUN', 'OCCDEV', 'OCCGA', 'OCCPROG', 'OCCSF', 'OPREHTOT',
       'COLEADBYR', 'LEADBYR', 'AUDIT', 'CDPTAXON', 'DUNS', 'FYEND',
       'ORGID', 'IRSDATE', 'ORGLAT', 'ORGLONG', 'NISPID', 'YRINC',
       'CLIAOTAG', 'RSCLIAOT', 'UNCLIAOT', 'OTHGRTAMT', 'OTHGRTN',
       'OTHNOPEXDEV', 'OTHNOPEXGA', 'OTHNOPEXPRG', 'OTHNOPEXSF',
       'OTHPRGDIGLIVE', 'OTHPRGDIGODEM', 'OTHPRGINP', 'NOOTPRG',
       'OTHPRGDIGPERF', 'OTHPRGPERF', 'PTEMPS', 'PTTURNCD', 'PTSEAS',
       'PTSEASTURN', 'WKSRDG', 'PNOEXTOT', 'LOCPREM', 'NATPREM',
       'WORLDCD', 'PVTLESSNDIG', 'PVTLESSNINP', 'TOBOOKINDIGLIVE',
       'TOBOOKINDIGODEM', 'TOBOOKIN', 'TOBOOKINDIGPERF', 'TOBOOKINPERFS',
       'TOSHOWSDIGLIVE', 'TOSHOWSDIGODEM', 'TOSHOWS', 'TOSHOWSDIGPERF',
       'TOSHOWSPERF', 'PFEEFUND', 'PFEEGA', 'PFEEPRG', 'PFEESF', 'EIN',
       'FISCSPEIN', 'MOCOUNT', 'CYRFOUND', 'SHLTREIN', 'SHLTRTYP',
       'SMORG', 'SHLTRYRFND', 'CZIP', 'CZIP4', 'PRGEXPERSF',
       'OFFEDDIGLIVE', 'OFFEDDIGODEM', 'OFFEDINP', 'OFFEDDIGPERF',
       'OFFEDINPPERF', 'RESIDENCIES', 'PUBLICARTWKS', 'PUBSDIG',
       'PUBSPHYS', 'WKSRDGDIGLIVE', 'WKSRDGDIGODEM', 'WKSRDGINP',
       'WKSRDGDIGPERF', 'WKSRDGINPPERF', 'RECVAG', 'RECVRS', 'RECEIVCD',
       'RECVUN', 'ATNDREVRS', 'ATNDREVTO', 'ATNDREVUN', 'PRSFEECNTRS',
       'PRSFEECNTTO', 'PRSFEECNTUN', 'CONTDIGTO', 'EARNDIGTO', 'GALSALTM',
       'GALSALTO', 'GALSALUN', 'MBRINRS', 'MBRINCINDRS', 'MBRINCTO',
       'MBRINCINDTO', 'MBRINC', 'MBRINCINDUN', 'MBRINCORGRS',
       'MBRINCORGTO', 'MBRINCORGUN', 'NPRGOREVRS', 'NPRGOREVTO',
       'NPRGOREVUN', 'OPRGRVNLRS', 'OPRGRVNLTO', 'OPRGRVNLUN', 'PUBSALTM',
       'PUBSALTO', 'PUBSALUN', 'RENTALRS', 'RENTALTO', 'RENTALCD',
       'ROYALRS', 'ROYALTO', 'ROYALCD', 'SPNSORRS', 'SPNSORTO', 'SPNSOR',
       'SUBINCRS', 'SUBINCTO', 'SUBINCCD', 'STINCRES', 'STINCTOT',
       'STINCUN', 'ED3INCRS', 'ED3INCTON', 'ED3INCCDN', 'SCHAWEXP',
       'SCHAWN', 'FILMSCRNDIGLIVE', 'FILMSCRNDIGODEM', 'FILMSCRNINP',
       'FILMSCRNDIGPERF', 'FILMSCRNINPPERF', 'SPEVINCRS', 'SPEVEXP',
       'SPEVINCTO', 'SPEVINCUN', 'SUBCOUNT', 'SUBRENEW', 'HISUBCD',
       'LOSUBCD', 'HISUBADM', 'LOSUBADM', 'CORPAVE', 'CORPRS', 'CORPTO',
       'CORPCD', 'FOUNDAVE', 'FOUNDRS', 'FOUNDTO', 'FOUNDCD', 'CITYRS',
       'CITYTO', 'CITYCD', 'COUNTYRS', 'COUNTYTO', 'COUNTYCD', 'FEDRS',
       'FEDTO', 'FEDCD', 'STATERS', 'STATETO', 'STATECD', 'GOVTO',
       'GOVRS', 'GOVCD', 'OTHINDAVE', 'OTHINDRS', 'OTHINDTO', 'OTHINDCD',
       'NARTRRS', 'NARTRTO', 'NARTRCD', 'UAORS', 'UAOTONEW', 'UAOCDNEW',
       'OTDONRS', 'OTDONTO', 'OTDON', 'TRIBALRS', 'TRIBALTO', 'TRIBAL',
       'TRUSTAVE', 'TRUSTRS', 'TRUSTTO', 'TRUSTCD', 'HITIX', 'LOTIX',
       'TOTASSAG', 'TOTASSETS', 'RSTOTASS', 'UNTOTASS', 'ALLATTTO',
       'KIDATTND', 'NETINCRS', 'NETINCUN', 'COMPRGPRDTO',
       'COMPRGPERFTO', 'TOTCNTTO', 'TOTCNTOPTO', 'CNTRS', 'TOTCNTCD',
       'DPRCNTO', 'TOTOFFRCD', 'TOTPRGS', 'OPEARNTO', 'OPEARNRS',
       'ERNNCAPCD', 'TOTSTAFF', 'PMEXSHOW', 'TMEXSHOW', 'TREXSHOW',
       'TOTEXTOX', 'TOTDEVCD', 'TOTGALF', 'TOTPRG', 'TOTEXSF',
       'FIELDTSHOW', 'FIELDTPERF', 'FRATNDTO', 'GUIDTPERF', 'GUIDTSHOW',
       'INKINDTO', 'FRATNDCD', 'TOATNDCD', 'PHATTTOT', 'INTEXTOT',
       'NOLECTUR', 'LECOCC', 'TOTLIAAG', 'TLNAX', 'TLNAAG', 'RSTLNA',
       'UNTLNA', 'TOTLIATOT', 'TONASSTO', 'TONASSAG', 'TOTRSX', 'TOTUNX',
       'NONOPEXPTOT', 'CAPTO', 'CAPRS', 'CAPUN', 'NPERSEXDEV',
       'NPERSEXGA', 'NPERSEXPRG', 'NPERSEXTOT', 'OCCTOT', 'OPEXPTOT',
       'OPTO', 'OPRS', 'OPUN', 'OTHNOPEXTOT', 'PDATNDTO', 'TOSHOWCD',
       'TOPERFCD', 'TOTCMPFM', 'PERSNEXDEV', 'PERSNEXGA', 'TOTCMP',
       'PERSNEXPRG', 'PERSNEXTOT', 'TOTEMPCD', 'TOTURNCD', 'PVTLESSONS',
       'PFEETOT', 'TOTOCCS', 'OFFEDSHW', 'PUBPRINT', 'WKSRDGPS',
       'TOTINTOX', 'TOTRSIN', 'TOTUNIN', 'FILMS', 'FILMSCRN', 'WRKCAPTO',
       'WRKCAPCD', 'EDDISTNCT', 'EDOCCR', 'PDACARCD', 'ARTSALTO',
       'VOLTOTCD', 'DEVSATCD', 'GASAT', 'SATPRG', 'SATSF', 'TOTSALCD',
       'WEBVIEWS', 'WEBVISITS', 'UWEBVIS', 'SPC1SQFT', 'SPC1ZIP',
       'SPC2SQFT', 'SPC2ZIP', 'SPC3SQFT', 'SPC3ZIP', 'SPC4SQFT',
       'SPC4ZIP', 'SPC5SQFT', 'SPC5ZIP', 'EDPROFDIGLIVE', 'EDPROFDIGODEM',
       'EDPROFINP', 'EDPROFDIGPERF', 'EDPROFINPPERF', 'SQFTTOT',
       'STATUS_CODE', 'CFYEND', 'SomeDate', 'PEIN', 'ORGYRFOUND', 'CYEAR']


def parse_cdp_df(rawdf,numericfields,throw_examples=False):
    #- replace '2020-04-20' --> 202004 format
    #rawdf['CFYEND']=rawdf['CFYEND'].str.replace('-','').str[:-2] 
    print("Checking for status")
    if 'STATUS_CODE' in rawdf.columns.values:
        rawdf=rawdf[rawdf['STATUS_CODE'].isin([2,3,5])] #- completed status
        print("Shape after throwing out incomplete samples: ", rawdf.shape )
    else:
        print("WARNING!!! No STATUS_CODE field found and filtered")
    dt_fields=['CFYEND','SomeDate','FYEND']
    print("Parsing dates fields: ",dt_fields)
    rawdf=parse_dt(rawdf,dt_fields)
    #- replacing '-' in the fields
    print("Parsing fields for unnecessary dashes")
    rawdf=remove_dash(rawdf,'EIN')
    rawdf=remove_dash(rawdf,'PEIN')
    rawdf=remove_dash(rawdf,'DUNS')
    #- remove hard carriage returns
    rawdf=rawdf.replace('\r\n','',regex=True)
    #- assign to the numeric fields
    print("Assigning numeric values to total fields: ", len(numericfields))
    rawdf=assign_numeric(rawdf,numericfields)
    #- remove cdpname examples/sample
    if throw_examples:
        print("Removing unreal-- examples,samples records entirely")
        print("Data shape before example removal: ", rawdf.shape)
        for val in ['Example','EXAMPLE','example','UAT','sample','Sample']:
            rawdf=remove_examples(rawdf,'CDPNAME',val)
    print("Parsed df shape", rawdf.shape)
    return rawdf

def parse_dt(indf, fields):
    df=indf.copy()
    for field in fields:
        #df[field]=df[field].astype(str)
        #df[field]=df[field].str.replace('-','').str[:6]
        df[field]=pd.to_datetime(df[field],errors='coerce').dt.strftime('%Y%m')
        df[field]=df[field].replace("nan",np.nan,regex=True)
    return df
    
def remove_dash(inputdf,field):
    #df[field].fillna("nan",inplace=True)
    df=inputdf.copy()
    df[field]=df[field].astype(str)
    df[field]=df[field].str.replace('-','',regex=False).str.replace('.0','',regex=False)
    df[field]=df[field].replace("nan",np.nan,regex=True)#.str.replace(' ','')
    return df

def assign_numeric(indf,fields):
    df=indf.copy()
    nofields=[]
    yesfields=[]
    for field in fields:
        if field not in df.columns.values:
            nofields.append(field)
        else:
            yesfields.append(field)
            df[field]=pd.to_numeric(df[field],errors='coerce',downcast='integer')
    print("Found fields total: ",len(yesfields))
    print("Missed fields total: ", len(nofields))
    print("All missed fields: ", nofields)
    return df

def remove_examples(indf,field,value):
    df=indf.copy()
    df[field]=df[field].astype(str)
    df=df[~df[field].str.contains(value)].reset_index(drop=True)
    df[field]=df[field].replace("nan",np.nan,regex=False)
    return df


def main(inputcdp,numericfields=numfields_st,throw_examples=False):
    print("Input file shape: ", inputcdp.shape)
    #-drop duplicates 
    inputcdp=inputcdp.drop_duplicates()
    print("shape after droping duplicates ", inputcdp.shape)
    #inputcdp.rename(columns={"StatusCD":"STATUS_CODE"},inplace=True)
    inputcdp['STATUS_CODE']=inputcdp['STATUS_CODE'].astype(int)
    parsedcdp=parse_cdp_df(inputcdp,numericfields,throw_examples=throw_examples)
    parsedcdp['MKTTOT']=parsedcdp[['MKTTOT','MKTTOTPRE']].sum(axis=1)
    parsedcdp['SomeDate']=pd.to_numeric(parsedcdp['SomeDate'],errors='coerce',downcast='integer')
    #- remove the carriage returns etc
    cdpfinal=parsedcdp.replace({r'\n':''}, regex=True)
    print("Final df shape: ", cdpfinal.shape)
    return cdpfinal