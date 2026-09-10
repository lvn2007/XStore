#include <argparse/argparse.hpp>
#include <filesystem>
#include <iostream>

#include "include/Networking/XStore_Curve.h"

namespace fs = std::filesystem;

namespace {

int RunGen(const std::string &name, const std::string &out_dir_arg, bool force) {
    fs::path out_dir = out_dir_arg;
    if (out_dir.empty()) {
        out_dir = (name == "server") ? fs::path("keys/server") : (fs::path("keys/clients") / name);
    }
    fs::path pub_path = out_dir / (name + ".pub");
    fs::path sec_path = out_dir / (name + ".key");
    if (!force && (fs::exists(pub_path) || fs::exists(sec_path))) {
        std::cerr << "xstore-keys: " << pub_path << " or " << sec_path
                  << " already exists -- pass --force to overwrite\n";
        return 1;
    }
    auto kp = XStoreCurveKeyPair::Generate();
    kp.SaveToFiles(pub_path, sec_path);
    std::cout << "Generated '" << name << "':\n"
              << "  public:  " << pub_path << " (share this)\n"
              << "  secret:  " << sec_path << " (keep private, mode 0600)\n";
    return 0;
}

int RunAllow(const std::string &name, const std::string &pubkey_file, const std::string &allowed_dir_arg, bool force) {
    fs::path allowed_dir = allowed_dir_arg;
    fs::path dest = allowed_dir / (name + ".pub");
    if (!force && fs::exists(dest)) {
        std::cerr << "xstore-keys: " << dest << " already exists -- pass --force to re-key '" << name << "'\n";
        return 1;
    }
    std::string z85;
    try {
        z85 = Z85ReadFile(pubkey_file);
    } catch (const std::exception &e) {
        std::cerr << "xstore-keys: " << e.what() << "\n";
        return 1;
    }
    Z85WriteFile(dest, z85, /*is_secret=*/false);
    std::cout << "Allowed '" << name << "' -> " << dest << "\n";
    return 0;
}

int RunRevoke(const std::string &name, const std::string &allowed_dir_arg) {
    fs::path dest = fs::path(allowed_dir_arg) / (name + ".pub");
    std::error_code ec;
    bool removed = fs::remove(dest, ec);
    if (removed) {
        std::cout << "Revoked '" << name << "'\n";
    } else {
        std::cout << "xstore-keys: no such identity '" << name << "' in " << allowed_dir_arg << " (nothing to revoke)\n";
    }
    return 0;
}

int RunList(const std::string &allowed_dir_arg) {
    fs::path allowed_dir = allowed_dir_arg;
    std::error_code ec;
    if (!fs::exists(allowed_dir, ec)) {
        std::cout << "(no allowlist directory at " << allowed_dir << ")\n";
        return 0;
    }
    bool any = false;
    for (const auto &entry : fs::directory_iterator(allowed_dir, ec)) {
        if (ec) break;
        if (entry.path().extension() != ".pub") continue;
        any = true;
        std::string z85;
        try {
            z85 = Z85ReadFile(entry.path());
        } catch (const std::exception &e) {
            std::cout << entry.path().stem().string() << "  <malformed: " << e.what() << ">\n";
            continue;
        }
        std::cout << entry.path().stem().string() << "  " << z85.substr(0, 6) << "..." << z85.substr(34) << "\n";
    }
    if (!any) std::cout << "(no identities allowed in " << allowed_dir << ")\n";
    return 0;
}

}  // namespace

int main(int argc, char *argv[]) {
    argparse::ArgumentParser program("xstore-keys");
    program.add_description("CURVE key housekeeping for XStore: generate identities, manage the server allowlist.");

    argparse::ArgumentParser gen_cmd("gen");
    gen_cmd.add_description("Generate a new Curve25519 keypair for an identity.");
    gen_cmd.add_argument("name");
    gen_cmd.add_argument("--out-dir").default_value(std::string(""))
        .help("output directory (default: keys/server/ for 'server', else keys/clients/<name>/)");
    gen_cmd.add_argument("--force").default_value(false).implicit_value(true)
        .help("overwrite an existing keypair for this name");

    argparse::ArgumentParser allow_cmd("allow");
    allow_cmd.add_description("Add an identity's public key to the server's allowlist.");
    allow_cmd.add_argument("name");
    allow_cmd.add_argument("--pubkey-file").required().help("path to the identity's .pub file");
    allow_cmd.add_argument("--allowed-dir").default_value(std::string("keys/allowed"));
    allow_cmd.add_argument("--force").default_value(false).implicit_value(true)
        .help("overwrite an existing allowlist entry for this name");

    argparse::ArgumentParser revoke_cmd("revoke");
    revoke_cmd.add_description("Remove an identity from the server's allowlist.");
    revoke_cmd.add_argument("name");
    revoke_cmd.add_argument("--allowed-dir").default_value(std::string("keys/allowed"));

    argparse::ArgumentParser list_cmd("list");
    list_cmd.add_description("List identities currently on the server's allowlist.");
    list_cmd.add_argument("--allowed-dir").default_value(std::string("keys/allowed"));

    program.add_subparser(gen_cmd);
    program.add_subparser(allow_cmd);
    program.add_subparser(revoke_cmd);
    program.add_subparser(list_cmd);

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception &err) {
        std::cerr << err.what() << "\n" << program;
        return 1;
    }

    try {
        if (program.is_subcommand_used(gen_cmd)) {
            return RunGen(gen_cmd.get<std::string>("name"), gen_cmd.get<std::string>("--out-dir"), gen_cmd.get<bool>("--force"));
        }
        if (program.is_subcommand_used(allow_cmd)) {
            return RunAllow(allow_cmd.get<std::string>("name"), allow_cmd.get<std::string>("--pubkey-file"),
                             allow_cmd.get<std::string>("--allowed-dir"), allow_cmd.get<bool>("--force"));
        }
        if (program.is_subcommand_used(revoke_cmd)) {
            return RunRevoke(revoke_cmd.get<std::string>("name"), revoke_cmd.get<std::string>("--allowed-dir"));
        }
        if (program.is_subcommand_used(list_cmd)) {
            return RunList(list_cmd.get<std::string>("--allowed-dir"));
        }
    } catch (const std::exception &e) {
        std::cerr << "xstore-keys: " << e.what() << "\n";
        return 1;
    }

    std::cerr << program;
    return 1;
}
