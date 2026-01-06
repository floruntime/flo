//! Shell Completion Generation
//!
//! Generates shell completion scripts for bash, zsh, fish, and powershell.

const std = @import("std");
const Allocator = std.mem.Allocator;
const cobra = @import("core.zig");
const Command = cobra.Command;
const Context = cobra.Context;

pub const Shell = enum {
    bash,
    zsh,
    fish,
    powershell,

    pub fn fromString(s: []const u8) ?Shell {
        if (std.mem.eql(u8, s, "bash")) return .bash;
        if (std.mem.eql(u8, s, "zsh")) return .zsh;
        if (std.mem.eql(u8, s, "fish")) return .fish;
        if (std.mem.eql(u8, s, "powershell") or std.mem.eql(u8, s, "ps")) return .powershell;
        return null;
    }
};

/// Generate completion script for the given shell
pub fn generate(allocator: Allocator, cmd: *Command, shell: Shell) ![]const u8 {
    return switch (shell) {
        .bash => try generateBash(allocator, cmd),
        .zsh => try generateZsh(allocator, cmd),
        .fish => try generateFish(allocator, cmd),
        .powershell => try generatePowershell(allocator, cmd),
    };
}

fn generateBash(allocator: Allocator, cmd: *Command) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();
    const name = cmd.name;

    try writer.print(
        \\# bash completion for {s}
        \\
        \\_{s}_completions() {{
        \\    local cur prev words cword
        \\    _init_completion || return
        \\
        \\    local commands="{s}"
        \\    local flags="{s}"
        \\
        \\    case "${{prev}}" in
        \\
    , .{ name, name, try getCommandNames(allocator, cmd), try getFlagNames(allocator, cmd) });

    // Add flag value completions
    for (cmd.flags.items) |flag| {
        if (flag.value_type != .bool) {
            try writer.print(
                \\        --{s})
                \\            return 0
                \\            ;;
                \\
            , .{flag.long});
        }
    }

    try writer.print(
        \\    esac
        \\
        \\    if [[ "${{cur}}" == -* ]]; then
        \\        COMPREPLY=($(compgen -W "${{flags}}" -- "${{cur}}"))
        \\    else
        \\        COMPREPLY=($(compgen -W "${{commands}}" -- "${{cur}}"))
        \\    fi
        \\}}
        \\
        \\complete -F _{s}_completions {s}
        \\
    , .{ name, name });

    return try buf.toOwnedSlice();
}

fn generateZsh(allocator: Allocator, cmd: *Command) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();
    const name = cmd.name;

    try writer.print(
        \\#compdef {s}
        \\
        \\_{s}() {{
        \\    local -a commands
        \\    local -a flags
        \\
        \\    commands=(
        \\
    , .{ name, name });

    // Add subcommands
    for (cmd.commands.items) |sub| {
        if (!sub.hidden) {
            try writer.print("        '{s}:{s}'\n", .{ sub.name, escapeZsh(sub.short) });
        }
    }

    try writer.writeAll(
        \\    )
        \\
        \\    flags=(
        \\
    );

    // Add flags
    for (cmd.flags.items) |flag| {
        if (!flag.hidden) {
            const desc = escapeZsh(flag.description);
            if (flag.short != 0) {
                try writer.print("        '(-{c} --{s})'{{{c},--{s}}}'[{s}]'\n", .{
                    flag.short,
                    flag.long,
                    flag.short,
                    flag.long,
                    desc,
                });
            } else {
                try writer.print("        '--{s}[{s}]'\n", .{ flag.long, desc });
            }
        }
    }

    try writer.print(
        \\    )
        \\
        \\    _arguments -s $flags '*::command:->command'
        \\
        \\    case $state in
        \\        command)
        \\            _describe -t commands 'commands' commands
        \\            ;;
        \\    esac
        \\}}
        \\
        \\_{s} "$@"
        \\
    , .{name});

    return try buf.toOwnedSlice();
}

fn generateFish(allocator: Allocator, cmd: *Command) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();
    const name = cmd.name;

    try writer.print(
        \\# fish completion for {s}
        \\
        \\function __{s}_needs_command
        \\    set -l cmd (commandline -opc)
        \\    if test (count $cmd) -eq 1
        \\        return 0
        \\    end
        \\    return 1
        \\end
        \\
        \\function __{s}_using_subcommand
        \\    set -l cmd (commandline -opc)
        \\    if test (count $cmd) -gt 1
        \\        if test $argv[1] = $cmd[2]
        \\            return 0
        \\        end
        \\    end
        \\    return 1
        \\end
        \\
    , .{ name, name, name });

    // Add subcommand completions
    for (cmd.commands.items) |sub| {
        if (!sub.hidden) {
            try writer.print(
                \\complete -c {s} -n '__{s}_needs_command' -a '{s}' -d '{s}'
                \\
            , .{ name, name, sub.name, escapeFish(sub.short) });
        }
    }

    // Add flag completions
    for (cmd.flags.items) |flag| {
        if (!flag.hidden) {
            const desc = escapeFish(flag.description);
            if (flag.short != 0) {
                try writer.print(
                    \\complete -c {s} -s {c} -l {s} -d '{s}'
                    \\
                , .{ name, flag.short, flag.long, desc });
            } else {
                try writer.print(
                    \\complete -c {s} -l {s} -d '{s}'
                    \\
                , .{ name, flag.long, desc });
            }
        }
    }

    return try buf.toOwnedSlice();
}

fn generatePowershell(allocator: Allocator, cmd: *Command) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();
    const name = cmd.name;

    try writer.print(
        \\# PowerShell completion for {s}
        \\
        \\Register-ArgumentCompleter -Native -CommandName {s} -ScriptBlock {{
        \\    param($wordToComplete, $commandAst, $cursorPosition)
        \\
        \\    $commands = @(
        \\
    , .{ name, name });

    // Add subcommands
    for (cmd.commands.items) |sub| {
        if (!sub.hidden) {
            try writer.print(
                \\        @{{ Name = '{s}'; Description = '{s}' }}
                \\
            , .{ sub.name, escapePowershell(sub.short) });
        }
    }

    try writer.writeAll(
        \\    )
        \\
        \\    $flags = @(
        \\
    );

    // Add flags
    for (cmd.flags.items) |flag| {
        if (!flag.hidden) {
            try writer.print(
                \\        @{{ Name = '--{s}'; Description = '{s}' }}
                \\
            , .{ flag.long, escapePowershell(flag.description) });
        }
    }

    try writer.print(
        \\    )
        \\
        \\    if ($wordToComplete -like '-*') {{
        \\        $flags | Where-Object {{ $_.Name -like "$wordToComplete*" }} | ForEach-Object {{
        \\            [System.Management.Automation.CompletionResult]::new($_.Name, $_.Name, 'ParameterValue', $_.Description)
        \\        }}
        \\    }} else {{
        \\        $commands | Where-Object {{ $_.Name -like "$wordToComplete*" }} | ForEach-Object {{
        \\            [System.Management.Automation.CompletionResult]::new($_.Name, $_.Name, 'ParameterValue', $_.Description)
        \\        }}
        \\    }}
        \\}}
        \\
    , .{});

    return try buf.toOwnedSlice();
}

fn getCommandNames(allocator: Allocator, cmd: *Command) ![]const u8 {
    var names = std.ArrayList(u8).init(allocator);
    defer names.deinit();

    for (cmd.commands.items, 0..) |sub, i| {
        if (!sub.hidden) {
            if (i > 0) try names.append(' ');
            try names.appendSlice(sub.name);
        }
    }

    return try names.toOwnedSlice();
}

fn getFlagNames(allocator: Allocator, cmd: *Command) ![]const u8 {
    var names = std.ArrayList(u8).init(allocator);
    defer names.deinit();

    for (cmd.flags.items, 0..) |flag, i| {
        if (!flag.hidden) {
            if (i > 0) try names.append(' ');
            try names.appendSlice("--");
            try names.appendSlice(flag.long);
        }
    }

    return try names.toOwnedSlice();
}

fn escapeZsh(s: []const u8) []const u8 {
    // TODO: proper escaping
    return s;
}

fn escapeFish(s: []const u8) []const u8 {
    // TODO: proper escaping
    return s;
}

fn escapePowershell(s: []const u8) []const u8 {
    // TODO: proper escaping
    return s;
}

/// Create a completion command that can be added to the root
pub fn completionCommand(allocator: Allocator, root_cmd: *Command) !*Command {
    _ = root_cmd;
    const cmd = Command.init(allocator, .{
        .name = "completion",
        .short = "Generate shell completion scripts",
        .long =
        \\Generate shell completion scripts for bash, zsh, fish, or powershell.
        \\
        \\To load completions:
        \\
        \\Bash:
        \\  $ source <(myapp completion bash)
        \\  # To load completions for each session:
        \\  $ myapp completion bash > /etc/bash_completion.d/myapp
        \\
        \\Zsh:
        \\  $ source <(myapp completion zsh)
        \\  # To load completions for each session:
        \\  $ myapp completion zsh > "${fpath[1]}/_myapp"
        \\
        \\Fish:
        \\  $ myapp completion fish | source
        \\  # To load completions for each session:
        \\  $ myapp completion fish > ~/.config/fish/completions/myapp.fish
        \\
        \\PowerShell:
        \\  PS> myapp completion powershell | Out-String | Invoke-Expression
        ,
        .args_validator = cobra.ArgValidators.exactArgs(1),
        .run = struct {
            fn run(ctx: *Context) cobra.Error!void {
                if (ctx.args.len < 1) {
                    ctx.printErr("Error: shell type required (bash, zsh, fish, powershell)\n", .{});
                    return error.InvalidArgs;
                }

                const shell = Shell.fromString(ctx.args[0]) orelse {
                    ctx.printErr("Error: unknown shell '{s}'. Use bash, zsh, fish, or powershell\n", .{ctx.args[0]});
                    return error.InvalidArgs;
                };

                const root = ctx.command.root();
                const script = generate(ctx.allocator, root, shell) catch {
                    ctx.printErr("Error generating completion script\n", .{});
                    return error.CommandFailed;
                };
                defer ctx.allocator.free(script);

                ctx.print("{s}", .{script});
            }
        }.run,
    });

    try cmd.addArg(.{
        .name = "shell",
        .description = "Shell type (bash, zsh, fish, powershell)",
        .required = true,
    });

    return cmd;
}
