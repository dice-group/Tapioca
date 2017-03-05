/*<!-- Author: Duong Trung Duong-->*/
// Select file by Button
document.getElementById('searchForm:documentFile-browser').onchange = function(event) {
	fileName = event.target.files[0].name;
	document.getElementById('searchForm:searchTextBox').value = "Uploaded file: " + fileName;
	document.getElementById('searchForm:searchTextBox').focus();	
};

function showWithID(id) {
	var element = document.getElementById(id);
	element.style.display = 'block';
}

function hideWithID(id) {
	var element = document.getElementById(id);
	element.style.display = 'none';
}