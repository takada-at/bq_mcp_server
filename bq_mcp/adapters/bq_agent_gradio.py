import json

from pydantic_ai.messages import ToolCallPart, ToolReturnPart

from bq_mcp.adapters.agent import agent as bq_agent
from bq_mcp.core.entities import ApplicationContext
from bq_mcp.repositories import cache_manager, config, log

try:
    import gradio as gr
except ImportError as e:
    raise ImportError(
        "Please install gradio with `pip install gradio`. You must use python>=3.10."
    ) from e


TOOL_TO_DISPLAY_NAME = {"bq_meta_api": "BigQuery Metadata API"}


async def stream_from_agent(prompt: str, chatbot: list[dict], past_messages: list):
    log_setting = log.init_logger()
    setting = config.init_setting()
    cache_data = await cache_manager.get_cached_data()
    context = ApplicationContext(
        settings=setting,
        cache_data=cache_data,
        log_setting=log_setting,
    )
    chatbot.append({"role": "user", "content": prompt})
    yield gr.Textbox(interactive=False, value=""), chatbot, gr.skip()
    async with bq_agent.run_stream(
        prompt, deps=context, message_history=past_messages
    ) as result:
        for message in result.new_messages():
            for call in message.parts:
                print(call)
                if isinstance(call, ToolCallPart):
                    if isinstance(call.args, str):
                        call_args = json.loads(call.args)
                    else:
                        call_args = call.args
                    metadata = {
                        "title": f"🛠️ Using {call.tool_name}",
                    }
                    if call.tool_call_id is not None:
                        metadata["id"] = call.tool_call_id

                    gr_message = {
                        "role": "assistant",
                        "content": "Parameters: " + str(call_args),
                        "metadata": metadata,
                    }
                    chatbot.append(gr_message)
                if isinstance(call, ToolReturnPart):
                    for gr_message in chatbot:
                        if (
                            gr_message is not None
                            and gr_message.get("metadata", {}).get("id", "")
                            == call.tool_call_id
                        ):
                            gr_message["content"] += (
                                f"\nOutput: {json.dumps(call.content)}"
                            )
                yield gr.skip(), chatbot, gr.skip()
        chatbot.append({"role": "assistant", "content": ""})
        async for message in result.stream_text():
            chatbot[-1]["content"] = message
            yield gr.skip(), chatbot, gr.skip()
        past_messages = result.all_messages()

        yield gr.Textbox(interactive=True), gr.skip(), past_messages


async def handle_retry(chatbot, past_messages: list, retry_data: gr.RetryData):
    new_history = chatbot[: retry_data.index]
    previous_prompt = chatbot[retry_data.index]["content"]
    past_messages = past_messages[: retry_data.index]
    async for update in stream_from_agent(previous_prompt, new_history, past_messages):
        yield update


def undo(chatbot, past_messages: list, undo_data: gr.UndoData):
    new_history = chatbot[: undo_data.index]
    past_messages = past_messages[: undo_data.index]
    return chatbot[undo_data.index]["content"], new_history, past_messages


def select_data(message: gr.SelectData) -> str:
    return message.value["text"]


with gr.Blocks() as demo:
    gr.HTML(
        """
<div style="display: flex; justify-content: center; align-items: center; gap: 2rem; padding: 1rem; width: 100%">
    <img src="https://ai.pydantic.dev/img/logo-white.svg" style="max-width: 200px; height: auto">
    <div>
        <h1 style="margin: 0 0 1rem 0">BigQuery Assistant</h1>
        <h3 style="margin: 0 0 0.5rem 0">
            Assistant to help create SQL
        </h3>
    </div>
</div>
"""
    )
    past_messages = gr.State([])
    chatbot = gr.Chatbot(
        label="Packing Assistant",
        type="messages",
        avatar_images=(None, "https://ai.pydantic.dev/img/logo-white.svg"),
        examples=[
            {"text": "Please show dataset list"},
        ],
    )
    with gr.Row():
        prompt = gr.Textbox(
            lines=1,
            show_label=False,
            placeholder="Please show dataset list",
        )
    generation = prompt.submit(
        stream_from_agent,
        inputs=[prompt, chatbot, past_messages],
        outputs=[prompt, chatbot, past_messages],
    )
    chatbot.example_select(select_data, None, [prompt])
    chatbot.retry(
        handle_retry, [chatbot, past_messages], [prompt, chatbot, past_messages]
    )
    chatbot.undo(undo, [chatbot, past_messages], [prompt, chatbot, past_messages])


if __name__ == "__main__":
    demo.launch()
