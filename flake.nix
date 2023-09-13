{
  description = "Simple flake with flake-utils";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachSystem [ "x86_64-linux" ] (system:
      let
        pkgs = import nixpkgs { inherit system; };

        hello = pkgs.hello;
        helloApp = flake-utils.lib.mkApp { drv = hello; };

      in {
        devShell = pkgs.mkShell {
          LD_LIBRARY_PATH = "${pkgs.zlib}/lib;${pkgs.leveldb}/lib;${pkgs.fuse3}/lib;";
          PKG_CONFIG_PATH = "${pkgs.fuse3}/lib/pkgconfig";
          inputsFrom = [ ];
          buildInputs = [ pkgs.zlib pkgs.leveldb pkgs.fuse3 ];
          nativeBuildInputs = [
            pkgs.pkg-config
            pkgs.haskell.compiler.ghc92
            pkgs.haskell.packages.ghc92.cabal-install
            pkgs.haskell.packages.ghc92.haskell-language-server

            pkgs.lz4
            pkgs.zstd
          ];
        };
      });
}
