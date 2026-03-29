var fs=require("fs"),p=require("path");
var dir=p.join(process.env.LOCALAPPDATA,"Temp","fte-dashboard");
var w=function(f,c){fs.writeFileSync(p.join(dir,f),c);console.log("wrote "+f+" ("+c.length+" bytes)");};
// Will be populated by the real content