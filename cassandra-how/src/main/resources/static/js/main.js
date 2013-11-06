var ractiveTweetResult;
var ractiveTweetList;

function tweet() {
	var sendobject = {
		username: document.querySelector("#tweetUserName").value,
		content: document.querySelector("#tweetContent").value
	}
    
    console.log("Sending", sendobject);
	
	jQuery.ajax({
	  type: "POST",
	  url: "/tweets",
	  data: JSON.stringify(sendobject),
	  contentType: "application/json; charset=UTF-8",
	  success: 
	  	function( data, textStatus, jqXHR ) {
			// success function
			console.log("Success", data);
			ractiveTweetResult.set('servermessage', data);
		}
	});
}

function loadTweets(username) {
	$.ajax({
	  dataType: "json",
	  url: "/tweets/" + username,
	  success:
		 function( data, textStatus, jqXHR ) {
			// success function
			console.log("Success", data);
			ractiveTweetList.set('username', username);
			ractiveTweetList.set('tweets', data);
		}
	});
}

function loadTweetsOfUser() {
	var username = $("#tweeterman").val();
	loadTweets(username);
}

function follow() {
	var sendobject = {
		username: document.querySelector("#follow-username").value,
		followed: document.querySelector("#follow-followed").value
	}
	$.ajax({
		type: "POST",
		contentType: "application/json; charset=UTF-8",
		url: "/follow",
		data: JSON.stringify(sendobject),
		success:
			function( data, textStatus, jqXHR ) {
				console.log(data);
				$("#follow-message").html(data);
			}		
	})
}

function unfollow() {
	var sendobject = {
		username: document.querySelector("#follow-username").value,
		followed: document.querySelector("#follow-followed").value
	}
	$.ajax({
		type: "POST",
		contentType: "application/json; charset=UTF-8",
		url: "/unfollow",
		data: JSON.stringify(sendobject),
		success:
			function( data, textStatus, jqXHR ) {
			    console.log(data);
				$("#follow-message").html(data);
			}		
	})
}

$( function() {
	ractiveTweetResult = new Ractive({
	    el: 'tweetAlerts',
	    template: '#tweetResultTemplate',
	    data: { servermessage: "[Do a tweet first]" }
	});
	
	ractiveTweetList = new Ractive({
		el: 'tweetList',
		template: '#tweetListTemplate'
	});
} );

