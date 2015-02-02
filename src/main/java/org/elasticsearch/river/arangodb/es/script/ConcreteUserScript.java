package org.elasticsearch.river.arangodb.es.script;

import java.util.Map;

import net.swisstech.arangodb.model.wal.WalEvent;

import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

public class ConcreteUserScript extends AbstractUserScript {

	private final ScriptService scriptService;
	private final CompiledScript script;

	public ConcreteUserScript(ScriptService scriptService, CompiledScript script) {
		this.scriptService = scriptService;
		this.script = script;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> executeScript(WalEvent event) {
		Map<String, Object> ctx = createContextObject(event);
		ExecutableScript exe = prepareScript(ctx);
		exe.run();
		return (Map<String, Object>) exe.unwrap(ctx);
	}

	private ExecutableScript prepareScript(Map<String, Object> ctx) {
		ExecutableScript exe = scriptService.executable(script, ctx);
		exe.setNextVar("ctx", ctx);
		return exe;
	}
}
