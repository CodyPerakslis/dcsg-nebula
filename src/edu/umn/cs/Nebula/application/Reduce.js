function p1() {
    // REDUCE FUNCTION
    var s = JSON.parse(nebula_task_details);
    var deps = []; // We always have only one dependency in MR
    for(var i = 0; i < nebula_dependency_data.length; i++) {
        deps[i] = JSON.parse(nebula_dependency_data[i])[0];
    }
    s['dependencies'] = deps;
    var input = JSON.stringify(s);

    NebulaModule.postMessage(input);
}

var timeout_reason = ""
	
function p2(message) {
    if (message.substring(0, 3) == 'LOG') { // starts with LOG
        message = split_message_and_update_info(message);
        console.log(message);
    } else if (message.substring(0, 13) == 'TIMEOUT-RESET') { // Reset timeout
        message = split_message_and_update_info(message);
        console.log(message);
        clearTimeout(notLoading);
        timeout_reason = message;
        notLoading = setTimeout("moduleFailed('notLoading');", 60000);
    } else if (message.substring(0, 14) == 'TIMEOUT-CANCEL') { // Reset timeout
        message = split_message_and_update_info(message);
        console.log(message);
        clearTimeout(notLoading);
        notLoading = null;
    } else {
        var ret = JSON.parse(message);
        var type = ret[0];
        var metadata = ret[1];

        var myForm = document.createElement("form");
        myForm.method = "POST";

        var metadataInput = document.createElement("input");
        metadataInput.setAttribute("name", "metadata");
        metadataInput.setAttribute("value", JSON.stringify(metadata));
        myForm.appendChild(metadataInput);

    	if(type == "done") {
    		var fileNames = ret[2];
    		myForm.action = "done";

    		var myInput = document.createElement("input");
    		myInput.setAttribute("name", "result");
    		myInput.setAttribute("value", JSON.stringify(fileNames));
    		myForm.appendChild(myInput);
    	} else if(type == "cancel") {
    		var message = ret[2];
    		myForm.action = "cancel";

    		var myInput = document.createElement("input");
    		myInput.setAttribute("name", "reason");
    		myInput.setAttribute("value", JSON.stringify({'message': message, 'info': {}}));
    		myForm.appendChild(myInput);
    	} else {
    		console.log(ret);
    	}

        document.body.appendChild(myForm);
        myForm.submit();
        document.body.removeChild(myForm);
    }
}

function moduleFailed(reason) {
	var myForm = document.createElement("form");
	myForm.method = "POST";
	myForm.action = "cancel";

	if(!reason || 0 === reason.length) {
		reason = timeout_reason;
	}

	var myInput = document.createElement("input");
	myInput.setAttribute("name", "reason");
	myInput.setAttribute("value", JSON.stringify({'message': reason, 'info': info}));
	myForm.appendChild(myInput);
	
    var metadataInput = document.createElement("input");
    metadataInput.setAttribute("name", "metadata");
    metadataInput.setAttribute("value", JSON.stringify([]));
    myForm.appendChild(metadataInput);

	console.log('Cancelling task. Reason: ' + reason);

    document.body.appendChild(myForm);
    myForm.submit();
    document.body.removeChild(myForm);
}