build:
	ocamlbuild -use-ocamlfind nbd.byte nbd.native

clean:
	ocamlbuild -use-ocamlfind -clean
.PHONY: build
         
