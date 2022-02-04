IMPORT _control, ut, Data_Services;

EXPORT FN_CopyFiles := MODULE

	EXPORT emails := 'kevin.l.logemann@lexisnexis.com, DataDevelopment-Ins@lexisnexis.com';

	EXPORT Keys_NonFCRA(string sname, string destinationGroup = _Control.TargetQueue.Prod_NonFCRA) := FUNCTION
					// Example sNames: 'thor_data400::key::advo::qa::addr_search1' or 'thor_data400::key::advo::qa::addr_search2'
					// Run on HTHOR when possible.
					// Nodes count must match (and can change between environments) for copy to work. Right now 400 to 400 - 5/2/2014
					// BWR Launch Code Below

		Alpha_Prod_Logical_FileName := nothor(fileservices.superfilecontents('~'+sname))[1].name;

		Boca_Prod_Logical_FileName := nothor(fileservices.superfilecontents(Data_Services.foreign_prod_boca + sname))[1].name;
			sAlpha_Prod_Logical_FileName := STRINGLIB.STRINGFIND(Boca_Prod_Logical_FileName, '::', 2) + 2;
		New_Alpha_Prod_Logical_FileName := Boca_Prod_Logical_FileName[sAlpha_Prod_Logical_FileName..];

		Need_New := Alpha_Prod_Logical_FileName <> New_Alpha_Prod_Logical_FileName;
		
		CopyKey := nothor(fileservices.Copy('~'+New_Alpha_Prod_Logical_FileName, destinationGroup, '~'+New_Alpha_Prod_Logical_FileName, _control.IPAddress.prod_thor_dali, , , , true, true, false, true, false, )); 

		sname_prefix := sname[..STRINGLIB.STRINGFIND(sname, '::', 3) + 1];
		sname_suffix := sname[STRINGLIB.STRINGFIND(sname, '::', 4)..];
		sname_father := sname_prefix + 'father' + sname_suffix;
		sname_grandfather := sname_prefix + 'grandfather' + sname_suffix;

		logstatus := DATASET([{sname, 'Logical File for ' + sname + ' is up to date: ' + New_Alpha_Prod_Logical_FileName}], {string supername, string status});
		logstatus2 := DATASET([{sname, 'Logical File for ' + sname + ' is now available: ' + New_Alpha_Prod_Logical_FileName}], {string supername, string status});
		
		LetsDoIt_ExclamationPoint := SEQUENTIAL(CopyKey, 
																						fileservices.promotesuperfilelist(['~'+sname, '~'+sname_father, '~'+sname_grandfather], '~'+New_Alpha_Prod_Logical_FileName, true),
																						OUTPUT(logstatus2, EXTEND, NAMED('_')),
																						fileservices.sendemail(emails, 'Advo: New ' + sname + ' Key on Alpha Prod', WORKUNIT + ': ' + New_Alpha_Prod_Logical_FileName));
		
	RETURN MAP(need_new => LetsDoIt_ExclamationPoint,
							  OUTPUT(logstatus, EXTEND, NAMED('__')));
	END;

END;

/* //BWR - Copy and Paste in BWR and run on HTHOR*

#WORKUNIT('name','Launch - Advo Key Copy');

lTargetESPAddress		:=	'10.194.12.2'; //Alpha Dev - 10.194.72.202, Alpha Prod - 10.194.12.2... One more place to change code at bottom
lTargetESPPort			:=	'8010';

	export	string	fSubmitNewWorkunit(string pECLText, string pCluster)	:=
	function
		string fWUCreateAndUpdate(string pECLText)	:=
		function

			rWUCreateAndUpdateRequest	:=
			record
				string										QueryText{xpath('QueryText'),maxlength(20000)}	:=	pECLText;
			end;

			rESPExceptions	:=
			record
				string		Code{xpath('Code'),maxlength(10)};
				string		Audience{xpath('Audience'),maxlength(50)};
				string		Source{xpath('Source'),maxlength(30)};
				string		Message{xpath('Message'),maxlength(200)};
			end;

			rWUCreateAndUpdateResponse	:=
			record
				string										Wuid{xpath('Workunit/Wuid'),maxlength(20)};
				dataset(rESPExceptions)		Exceptions{xpath('Exceptions/ESPException'),maxcount(110)};
			end;

			dWUCreateAndUpdateResult	:=	soapcall('http://' + lTargetESPAddress + ':' + lTargetESPPort + '/WsWorkunits',
																						 'WUUpdate',
																						 rWUCreateAndUpdateRequest,
																						 rWUCreateAndUpdateResponse,
																						 xpath('WUUpdateResponse')
																						);

			return	dWUCreateAndUpdateResult.WUID;
			
		end;

		fWUSubmit(string pWUID, string pCluster)	:=
		function
			rWUSubmitRequest	:=
			record
				string										WUID{xpath('Wuid'),maxlength(20)}										:=	pWUID;
				string										Cluster{xpath('Cluster'),maxlength(30)}							:=	pCluster;
				// string										Queue{xpath('Queue'),maxlength(30)}									:=	pQueue;
				string										Snapshot{xpath('Snapshot'),maxlength(10)}						:=	'';
				string										MaxRunTime{xpath('MaxRunTime'),maxlength(10)}				:=	'0';
				string										Block{xpath('BlockTillFinishTimer'),maxlength(10)}	:=	'0';
			end;

			rWUSubmitResponse	:=
			record
				string										Code{xpath('Code'),maxlength(10)};
				string										Audience{xpath('Audience'),maxlength(50)};
				string										Source{xpath('Source'),maxlength(30)};
				string										Message{xpath('Message'),maxlength(200)};
			end;

			dWUSubmitResult	:=	soapcall('http://' + lTargetESPAddress + ':' + lTargetESPPort + '/WsWorkunits',//http://10.193.211.1:8010/WsWorkunits/',
																	 'WUSubmit',
																	 rWUSubmitRequest,
																	 rWUSubmitResponse,
																	 xpath('WUSubmitResponse/Exceptions/Exception')
																	);

			return	dWUSubmitResult;
		end;

		string	lWUIDCreated	:=	fWUCreateAndUpdate(pECLText);
		dExceptions						:=	fWUSubmit(lWUIDCreated, pCluster);
		string	ReturnValue		:=	if(dExceptions.Code = '',
																 lWUIDCreated,
																 ''
																);
		return	dExceptions.Code;//return	ReturnValue;
	end;

lECL 	:=	'#WORKUNIT(\'name\',\'Advo Copy\');\n PARALLEL(\n Advo.FN_CopyFiles.Keys_NonFCRA(\'thor_data400::key::advo::qa::addr_search1\'),\n Advo.FN_CopyFiles.Keys_NonFCRA(\'thor_data400::key::advo::qa::addr_search2\')) :\n FAILURE(fileservices.sendemail(Advo.FN_CopyFiles.emails,\'FAILURE: Advo Key File Copy\',WORKUNIT + \': \' + FAILMESSAGE));';

fSubmitNewWorkunit(lECL, 'hthor_prod') : WHEN(CRON('0 20 * * *')); //Alpharetta Dev - hthor_dev, Alpharetta Prod - hthor_prod

*/